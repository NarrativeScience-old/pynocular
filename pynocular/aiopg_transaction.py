"""Module for aiopg transaction utils"""
import asyncio
import sys
from typing import Dict, Optional, Union

import aiocontextvars as contextvars
from aiopg.sa.connection import SAConnection
import aiopg.sa.engine

transaction_connections_var = contextvars.ContextVar(
    "transaction_connections", default={}
)


def get_current_task() -> asyncio.Task:
    """Get the current task when this method is called

    Returns:
        The current task the method is called in

    """
    if sys.version_info.major == 3 and sys.version_info.minor > 7:
        # If this is version 3.7 or higher then use the new function to get the current task
        return asyncio.current_task()
    else:
        return asyncio.Task.current_task()


class LockedConnection(SAConnection):
    """A wrapper connection class that won't make multiple queries at once"""

    def __init__(self, connection: SAConnection) -> None:
        """Create a new LockedConnection

        Args:
            connection: The connection to wrap

        """
        self._conn = connection
        self.lock = asyncio.Lock()

    async def execute(self, *args, **kwargs):
        """Wrapper around the `execute` method of the wrapped SAConnection"""
        async with self.lock:
            return await self._conn.execute(*args, **kwargs)

    def __getattr__(self, attr):
        """Except for execute, all other attributes should pass through"""
        return getattr(self._conn, attr)


class TaskContextConnection:
    """Interface for managing a connection entry on the asyncio Task context

    The current asyncio.Task has a context attribute that keeps track of various keys.
    We'll use this to store the open connection so we can perform our nested/conditional
    transaction logic in :py:class:`transaction`. The actual value stored on the context
    is a dict of connections keyed by the engine.
    """

    def __init__(self, connection_key: str) -> None:
        """Initializer

        Args:
            connection_key: Key for getting/setting/clearing from the connection map

        """
        self.connection_key = connection_key
        self._token: Optional[contextvars.Token] = None

        # Set the asyncio task context if it's not set already. We'll look in the
        # context for an open connection.
        task = get_current_task()
        if not hasattr(task, "context"):
            task.context = contextvars.copy_context()

    @classmethod
    def _get_connections(cls) -> Dict[str, LockedConnection]:
        """Get the map of connections from the task context"""
        global transaction_connections_var
        return transaction_connections_var.get()

    def get(self) -> Optional[LockedConnection]:
        """If there is already a connection stored, get it"""
        return self._get_connections().get(self.connection_key)

    def set(self, conn: LockedConnection) -> contextvars.Token:
        """Set the connection on the context

        Args:
            conn: Connection to store

        Returns:
            contextvars token used to reset the var in :py:meth:`.clear`

        """
        global transaction_connections_var
        connections = self._get_connections()
        connections[self.connection_key] = conn
        token = transaction_connections_var.set(connections)
        self._token = token
        return token

    def clear(self) -> None:
        """Clear the connection from the context"""
        if not self._token:
            raise ValueError("Token must be defined")

        global transaction_connections_var
        transaction_connections_var.reset(self._token)


class transaction:
    """A context manager to collect nested calls in a transaction

    To use, anywhere you want to have queries put into a transaction, do

        async with transaction(aiopg_engine) as trx:
            ...

    The resulting trx object can be used just like a connection you would
    get from `aiopg_engine.acquire()`, but any nested usages of this decorator
    will ensure that we do not deadlock from nested acquire calls, and do not
    run into errors where we attempt to use the same connection to make
    multiple calls at once.
    NB: It does this by ensuring that we get the same connection object and
    execute serially, so you only want to use this in cases where you are
    worried about these issues.

    For example, using just aiopg, this is an error:

        async with engine.acquire() as conn:
            await asyncio.gather(
                conn.execute(TABLE.insert().values(id=uuid(), name="foo")),
                conn.execute(TABLE.insert().values(id=uuid(), name="bar")))

    But using this class, we will not error:

        async with transaction(engine) as conn:
            await asyncio.gather(
                conn.execute(TABLE.insert().values(id=uuid(), name="foo")),
                conn.execute(TABLE.insert().values(id=uuid(), name="bar")))

    Note:
        There are limits to the transaction rollback protection that this
        context manager affords. Specifically, a known failure case can be
        encountered if a DB connection is created by calling `Engine.acquire`
        rather than `transaction(Engine)`, even if the call to `acquire` is
        made within a transaction context. For more information, see:
        :py:module:`python_core.tests.functional
        .test_aiopg_transaction_integrity`.

    """

    def __init__(self, engine: aiopg.sa.engine.Engine) -> None:
        """Create a new transaction context

        Args:
            engine: Database engine for making connections

        """
        self._engine = engine

        # Is this the outer-most transaction context?
        # If so, this will be set to true in `__aenter__`
        self._top = False

        # If we have started a transaction, store it here
        self._trx = None

        # Initiatize an interface for managing the connection on the asyncio task context
        self.task_connection = TaskContextConnection(str(engine))

    async def __aenter__(self) -> LockedConnection:
        """Establish the transaction context

        Figure out if this is the top level context. If so, get a connection
        and start a transaction. If not, then just grab the stored connection.
        """
        conn = self.task_connection.get()
        if not conn:
            # There is no stored connection in this context, so this must be
            # the top level call.
            self._top = True
            # Create the connection
            conn = LockedConnection(await self._engine.acquire())
            self.task_connection.set(conn)
            # Start a transaction
            try:
                self._trx = await conn.begin()
            except Exception:
                await conn.close()
                self.task_connection.clear()
                raise
        return conn

    async def __aexit__(self, exc_type, exc_value, tb) -> None:
        """Exit the transaction context

        If this is the top level context, then commit the transaction (unless
        there was an error, in which case we should rollback instead).
        If this is not the top level context, we don't need to do anything,
        since everything will be committed or rolled back by that top level
        context.
        """
        if self._top:
            # We may have gotten here from an error, in which case it is
            # possible that we are also awaiting for a query to finish
            # executing. So before rolling back the connection, make sure we
            # can acquire the connection lock to ensure nothing else is
            # executing
            conn = self.task_connection.get()
            async with conn.lock:
                try:
                    if exc_type:  # There was an exception
                        await self._trx.rollback()
                    else:
                        await self._trx.commit()
                finally:
                    self.task_connection.clear()
                    await conn.close()


class ConditionalTransaction(transaction):
    """Context manager to conditionally collect nested calls in a transaction

    This context manager allows you to conditionally execute code in a
    transaction if nested within another transaction. If it is the top level
    "transaction", this will behave like a standard `engine.acquire()`. Usage
    is otherwise the same as for the parent transaction class.

    Examples:
        This Will behave the same as `engine.acquire`, assuming this is not
        nested under a transaction elsewhere

            async with ConditionalTransaction(engine) as trx:
                ...

        This will behave as a nested transaction:

            async with transaction(engine) as outer_trx:
                async with ConditionalTransaction(engine) as inner_trx:
                    ...

    """

    def __init__(self, engine: aiopg.sa.engine.Engine) -> None:
        """Initialize the context manager

        Args:
            engine: An aiopg engine

        """
        super().__init__(engine)
        # The connection object, if functioning as standard connection
        self._conn = None

    async def __aenter__(self) -> Union[LockedConnection, SAConnection]:
        """Conditionally establish the transaction context

        Returns:
            Either a locked connection or a standard connection, depending on
            whether this context manager is nested under a transaction.

        """
        conn = self.task_connection.get()
        # If there is already a connection stored, act as a transaction
        if conn:
            return await super().__aenter__()
        # Otherwise behave as a standard connection
        self._conn = await self._engine.acquire()
        return self._conn

    async def __aexit__(self, exc_type, exc_value, tb) -> None:
        """Exit the transaction context"""
        if self._conn is not None:
            await self._conn.close()
        else:
            await super().__aexit__(exc_type, exc_value, tb)
