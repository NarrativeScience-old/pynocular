"""Test for TaskContextConnection"""
from unittest.mock import Mock

import pytest

from pynocular.aiopg_transaction import LockedConnection, TaskContextConnection


@pytest.fixture()
def locked_connection():
    """Return a locked connection"""
    return LockedConnection(Mock())


@pytest.mark.asyncio()
async def test_task_context_connection_set_clear(locked_connection) -> None:
    """Test that we can set and clear the connection"""

    context_conn = TaskContextConnection("key1")
    context_conn.set(locked_connection)
    test_conn = context_conn.get()
    assert test_conn == locked_connection

    context_conn.clear()
    # No connection should exist now
    test_conn = context_conn.get()
    assert test_conn is None


@pytest.mark.asyncio()
async def test_task_context_connection_shared(locked_connection) -> None:
    """Test that we can share context across instances"""

    context_conn = TaskContextConnection("key1")
    context_conn.set(locked_connection)
    test_conn = context_conn.get()
    assert test_conn == locked_connection

    # Create another instance that should share the connection
    context_conn2 = TaskContextConnection("key1")
    test_conn2 = context_conn2.get()
    assert test_conn2 == locked_connection

    context_conn.clear()
    # No connection should exist on either connection
    test_conn = context_conn.get()
    assert test_conn is None
    test_conn2 = context_conn2.get()
    assert test_conn2 is None
