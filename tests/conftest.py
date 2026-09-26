"""Shared test configuration.

The async engine is created at import time and binds asyncpg connections to
whatever event loop is active, so the whole suite (module-scoped seeding
fixture, function-scoped test clients and their concurrent requests) must run
on ONE loop. We override the ``event_loop`` fixture at session scope.
"""
from __future__ import annotations

import asyncio

import pytest


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        yield loop
    finally:
        loop.close()
