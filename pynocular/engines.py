"""Functions for getting database engines and connections"""
import asyncio
from enum import Enum
from functools import wraps
import logging
import ssl
from typing import Any, Callable, Dict, NamedTuple, Optional, Tuple, Union
from urllib.parse import parse_qs

from aiopg import Connection as AIOPGConnection
from aiopg.sa import create_engine, Engine
from asyncpg.connection import Connection as AsyncPGConnection
from asyncpg.exceptions import ConnectionDoesNotExistError
from asyncpg.pool import Pool
from asyncpgsa import create_pool
import backoff

from pynocular.aiopg_transaction import (
    ConditionalTransaction,
    transaction as Transaction,
)
from pynocular.config import DB_POOL_MAX_SIZE, DB_POOL_MIN_SIZE, POOL_RECYCLE

logger = logging.getLogger(__name__)

_engines: Dict[Tuple[str, str], Engine] = {}
_pools: Dict[Tuple[str, str], Pool] = {}


async def get_aiopg_engine(
    conn_str: str,
    enable_hstore: bool = True,
    force: bool = False,
    application_name: str = None,
    if_exists: bool = False,
) -> Optional[Engine]:
    """Returns the aiopg SQLAlchemy connection engine for a given connection string.

    This function lazily creates the connection engine if it doesn't already
    exist. Callers of this function shouldn't close the engine. It will be
    closed automatically when the process exits.

    This function exists to keep a single engine (and thus a single connection
    pool) per database, which prevents us from maxing out the number of
    connections the database server will give us.

    We include the hash of the event loop in the cache key because otherwise, if the
    event loop closes, the cached engine will raise an exception when it's used.

    Args:
        conn_str: The connection string for the engine
        enable_hstore: determines if the hstore should be enabled on the database.
            Redshift requires this to be disabled.
        force: Force the creation of the engine regardless of the cache
        application_name: Arbitrary string that shows up in queries to the
            ``pg_stat_activity`` view for tracking the source of database connections.
        if_exists: Only return the engine if it already exists

    Returns:
        aiopg engine for the connection string

    """
    global _engines
    logger.debug("Attempting to get DB engine")
    loop_hash = str(hash(asyncio.get_event_loop()))
    cache_key = (loop_hash, conn_str)
    engine = _engines.get(cache_key)

    if if_exists and engine is None:
        return None

    if engine is None or force or engine.closed:
        engine = await create_engine(
            conn_str,
            enable_hstore=enable_hstore,
            application_name=application_name,
            pool_recycle=POOL_RECYCLE,
        )
        _engines[cache_key] = engine
        logger.debug(f"DB engine created successfully: {engine}")

    logger.debug("DB engine retrieved")
    return engine


@backoff.on_exception(
    backoff.expo, ConnectionDoesNotExistError, max_tries=8, jitter=backoff.random_jitter
)
async def get_asyncpgsa_pool(
    conn_str: str,
    force: bool = False,
    application_name: str = None,
    if_exists: bool = False,
) -> Optional[Pool]:
    """Returns the asyncpgsa connection pool for a given connection string.

    This function lazily creates the connection pool if it doesn't already
    exist. Callers of this function shouldn't close the pool. It will be
    closed automatically when the process exits.

    This function exists to keep a single pool per database, which prevents us
    from maxing out the number of connections the database server will give us.

    We include the hash of the event loop in the cache key because otherwise, if the
    event loop closes, the cached pool will raise an exception when it's used.

    Args:
        conn_str: The connection string for the engine
        force: Force the creation of the pool regardless of the cache
        application_name: Arbitrary string that shows up in queries to the
            ``pg_stat_activity`` view for tracking the source of database connections.
        if_exists: Only return the pool if it already exists

    Returns:
        The aiopgsa pool for that connection

    """
    global _pools
    logger.debug("Attempting to get/create the db pool")
    loop_hash = str(hash(asyncio.get_event_loop()))
    cache_key = (loop_hash, conn_str)
    pool = _pools.get(cache_key)

    # Split the query params from the connection string
    parsed_conn = conn_str.split("?")
    conn_str = parsed_conn[0]
    ssl_cert_path = None

    if len(parsed_conn) > 1:
        query_params = parse_qs(parsed_conn[1])
        if "sslrootcert" in query_params:
            ssl_cert_path = query_params["sslrootcert"][0]

    if if_exists and pool is None:
        return None

    if pool is None or force or pool._closed:
        options = {}
        if application_name is not None:
            options["server_settings"] = {"application_name": application_name}
        if ssl_cert_path is not None:
            ssl_context = ssl.create_default_context()
            ssl_context.load_verify_locations(cafile=ssl_cert_path)
            options["ssl"] = ssl_context  # type: ignore
        try:
            pool = await create_pool(
                conn_str,
                min_size=DB_POOL_MIN_SIZE,
                max_size=DB_POOL_MAX_SIZE,
                max_inactive_connection_lifetime=POOL_RECYCLE,
                **options,
            )
        except Exception as e:
            logger.error(
                {
                    "description": "Failed to create asyncpg connection pool",
                    "application_name": application_name,
                    "min_size": DB_POOL_MIN_SIZE,
                    "max_size": DB_POOL_MAX_SIZE,
                    "force": force,
                    "if_exists": if_exists,
                    "error_msg": repr(e),
                }
            )
            raise e

        _pools[cache_key] = pool
        logger.debug(f"Created new asyncpg pool: {pool}")

    logger.debug("Connection pool successfully retrieved")
    return pool


class DatabaseType(Enum):
    """Database type to differentiate engines and pools"""

    aiopg_engine = "aiopg_engine"
    asyncpgsa_pool = "asyncpgsa_pool"


class DBInfo(NamedTuple):
    """Data class for a database's connection information"""

    engine_type: DatabaseType
    connection_string: str
    enable_hstore: bool = True


class DBEngine:
    """Wrapper over database engine types"""

    @classmethod
    async def _get_engine_or_pool(
        cls,
        db_info: DBInfo,
        force: bool = False,
        application_name: str = None,
        if_exists: bool = False,
    ) -> Optional[Union[Engine, Pool]]:
        """Get an aiopg engine or asyncpg pool depending on the database configuration.

        Args:
            db_info: Information for making the database connection
            force: Force the creation of the pool regardless of the cache
            application_name: Arbitrary string that shows up in queries to the
                ``pg_stat_activity`` view for tracking the source of database
                connections.
            if_exists: Only return the engine or pool if it already exists

        Returns:
            database engine or pool

        Raises:
            :py:exec:`ValueError` if the database type isn't supported

        """
        if db_info.engine_type == DatabaseType.aiopg_engine:
            return await get_aiopg_engine(
                db_info.connection_string,
                enable_hstore=db_info.enable_hstore,
                force=force,
                application_name=application_name,
                if_exists=if_exists,
            )
        elif db_info.engine_type == DatabaseType.asyncpgsa_pool:
            return await get_asyncpgsa_pool(
                db_info.connection_string,
                force=force,
                application_name=application_name,
                if_exists=if_exists,
            )

        raise ValueError(f"Unsupported database type: {db_info.engine_type}")

    @classmethod
    async def get_engine(
        cls, db_info: DBInfo, force: bool = False, application_name: str = None
    ) -> Engine:
        """Get a SQLAlchemy connection engine for a given database alias.

        See :py:func:`.get_engine` for more details.

        Args:
            db_info: database connection information
            force: Force the creation of the pool regardless of the cache
            application_name: Arbitrary string that shows up in queries to the
                ``pg_stat_activity`` view for tracking the source of database
                connections.

        Returns:
            database engine

        """
        return await cls._get_engine_or_pool(
            db_info, force=force, application_name=application_name
        )

    @classmethod
    async def get_pool(
        cls, db_info: DBInfo, force: bool = False, application_name: str = None
    ) -> Pool:
        """Get a asyncpgsa connection pool for a given database alias.

        See :py:func:`.get_pool` for more details.

        Args:
            db_info: database connection information
            force: Force the creation of the pool regardless of the cache
            application_name: Arbitrary string that shows up in queries to the
                ``pg_stat_activity`` view for tracking the source of database
                connections.

        Returns:
            database connection pool

        """
        if db_info.engine_type != DatabaseType.asyncpgsa_pool:
            raise ValueError(f"Cannot get a connection pool with {db_info.engine_type}")
        return await cls._get_engine_or_pool(
            db_info, force=force, application_name=application_name
        )

    @classmethod
    async def acquire(
        cls, db_info: DBInfo
    ) -> Union[AIOPGConnection, AsyncPGConnection]:
        """Acquire a SQLAlchemy connection for a given database alias.

        This is a convenience function that first gets/creates the engine or pool then
        calls acquire. This returns a context manager.

        Args:
            db_info: database connection information

        Returns:
            context manager that yields the connection

        """
        engine = await cls._get_engine_or_pool(db_info)
        return engine.acquire()

    @classmethod
    async def transaction(
        cls, db_info: DBInfo, is_conditional: bool = False
    ) -> Union[ConditionalTransaction, Transaction]:
        """Acquire a SQLAlchemy transaction for a given database alias.

        This is a convenience function that first gets/creates the engine then calls
        ConditionalTransaction. This returns a context manager.

        Args:
            db_info: database connection information
            is_conditional: If true, returns a conditional transaction.

        Returns:
            Transaction or ConditionalTransaction for use as a context manager

        """
        engine = await cls._get_engine_or_pool(db_info)
        return ConditionalTransaction(engine) if is_conditional else Transaction(engine)

    @classmethod
    def open_transaction(cls, db_info: DBInfo) -> Callable:
        """Decorator that wraps the function call in a database transaction

        Args:
            database_alias: The database alias to use for the transaction

        Returns:
            The wrapped function call

        """

        def parameterized_decorator(fn: Callable) -> Callable:
            """Function that will create the wrapper function

            Args:
                fn: The function to wrap

            Returns:
                The wrapped function

            """

            @wraps(fn)
            async def wrapped_funct(*args: Any, **kwargs: Any) -> Any:
                """The actual wrapper function

                Args:
                    args: The argument calls to the wrapped function
                    kwargs: The keyword args to the wrapped function

                Returns:
                    The result of the function

                """
                async with await DBEngine.transaction(db_info, is_conditional=False):
                    ret = await fn(*args, **kwargs)
                return ret

            return wrapped_funct

        return parameterized_decorator

    @classmethod
    async def close(cls, db_info: DBInfo) -> None:
        """Close existing database engines and pools

        Args:
            db_info: database connection information

        """
        logger.info("Closing database engine")
        pool_engine = await cls._get_engine_or_pool(db_info, if_exists=True)
        if pool_engine is None:
            # The engine/pool doesn't exist so nothing to close
            pass
        elif db_info.engine_type == DatabaseType.asyncpgsa_pool:
            await pool_engine.close()
        else:
            pool_engine.close()
            await pool_engine.wait_closed()
