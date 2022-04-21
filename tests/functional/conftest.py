"""Contains shared functional test fixtures"""

import asyncio

import pytest


@pytest.fixture(scope="session")
def event_loop():
    """Returns the event loop so we can define async, session-scoped fixtures"""
    return asyncio.get_event_loop()
