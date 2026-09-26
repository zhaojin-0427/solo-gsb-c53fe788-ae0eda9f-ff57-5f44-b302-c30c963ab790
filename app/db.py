"""Async SQLAlchemy engine / session management."""
from __future__ import annotations

from collections.abc import AsyncIterator

from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.config import get_settings

_settings = get_settings()

engine = create_async_engine(
    _settings.database_url,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
    future=True,
)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def get_session() -> AsyncIterator[AsyncSession]:
    """FastAPI dependency: yields a session and commits on success."""
    async with SessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


# Columns added to idempotency_keys when upgrading a database created by an
# earlier revision of the service. (name -> DDL type)
_IDEMPOTENCY_ADDED_COLUMNS = {
    "state": "VARCHAR(16) NOT NULL DEFAULT 'COMPLETED'",
    "attempts": "INTEGER NOT NULL DEFAULT 1",
    "locked_at": "TIMESTAMPTZ",
    "completed_at": "TIMESTAMPTZ",
    "run_token": "VARCHAR(36)",
}


async def ensure_schema(target_engine: AsyncEngine) -> None:
    """Create tables and upgrade the idempotency ledger in place.

    ``create_all`` never alters existing tables, so databases provisioned by
    the previous release need the new placeholder columns added explicitly.
    """
    from app.models import Base

    async with target_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

        table = await conn.execute(
            text(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_name = 'idempotency_keys'"
            )
        )
        if table.first() is None:  # pragma: no cover - create_all made it
            return

        cols = {
            r[0]
            for r in (
                await conn.execute(
                    text(
                        "SELECT column_name FROM information_schema.columns "
                        "WHERE table_name = 'idempotency_keys'"
                    )
                )
            ).fetchall()
        }
        for name, ddl in _IDEMPOTENCY_ADDED_COLUMNS.items():
            if name not in cols:
                await conn.execute(
                    text(
                        f"ALTER TABLE idempotency_keys ADD COLUMN {name} {ddl}"
                    )
                )

        # Earlier release limited scope to VARCHAR(32).
        await conn.execute(
            text(
                "ALTER TABLE idempotency_keys ALTER COLUMN scope TYPE VARCHAR(160)"
            )
        )
        # Legacy rows are finished responses; response_body/status_code used
        # to be NOT NULL -- loosen so PENDING placeholders fit.
        await conn.execute(
            text(
                "ALTER TABLE idempotency_keys "
                "ALTER COLUMN response_body DROP NOT NULL"
            )
        )
        await conn.execute(
            text(
                "ALTER TABLE idempotency_keys "
                "ALTER COLUMN status_code DROP NOT NULL"
            )
        )
        # Backfill timestamps for columns added by the upgrade above.
        await conn.execute(
            text(
                "UPDATE idempotency_keys SET locked_at = created_at "
                "WHERE locked_at IS NULL"
            )
        )
        await conn.execute(
            text(
                "UPDATE idempotency_keys SET completed_at = created_at "
                "WHERE state = 'COMPLETED' AND completed_at IS NULL"
            )
        )
        await conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_idempotency_state "
                "ON idempotency_keys (state)"
            )
        )


async def ping() -> bool:
    async with engine.connect() as conn:
        await conn.execute(text("SELECT 1"))
    return True


async def dispose_engine() -> None:
    await engine.dispose()
