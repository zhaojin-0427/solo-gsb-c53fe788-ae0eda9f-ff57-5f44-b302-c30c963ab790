"""FastAPI application entry point."""
from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from sqlalchemy import select

from app.config import get_settings
from app.db import SessionLocal, dispose_engine, engine, ensure_schema, get_session, ping
from app.models import Policy
from app.routers import ingest, policies, traces
from app.services.default_policy import DEFAULT_POLICY_NAME, DEFAULT_POLICY_SPEC
from app.services.traces import sweep_once
from app.sweeper import run_sweeper

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("tail_sampling")
settings = get_settings()


async def init_db() -> None:
    # Postgres may still be starting when the API boots.
    last_error: Exception | None = None
    for attempt in range(30):
        try:
            await ensure_schema(engine)
            break
        except Exception as exc:  # pragma: no cover - startup retry
            last_error = exc
            await asyncio.sleep(1)
    else:  # pragma: no cover
        raise RuntimeError(f"database unreachable: {last_error}")

    if settings.seed_default_policy:
        async with SessionLocal() as session:
            exists = (
                await session.execute(select(Policy).where(Policy.is_active.is_(True)))
            ).scalar_one_or_none()
            if exists is None:
                session.add(
                    Policy(name=DEFAULT_POLICY_NAME, spec=DEFAULT_POLICY_SPEC, is_active=True)
                )
                await session.commit()
                logger.info("seeded default policy %s", DEFAULT_POLICY_NAME)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    stop = asyncio.Event()
    sweeper_task: asyncio.Task | None = None
    if settings.enable_sweeper:
        sweeper_task = asyncio.create_task(run_sweeper(stop))
    try:
        yield
    finally:
        stop.set()
        if sweeper_task is not None:
            await sweeper_task
        await dispose_engine()


app = FastAPI(
    title="Distributed Tracing Tail Sampling Decision API",
    version="1.0.0",
    description=(
        "Receives out-of-order spans, rebuilds traces at window close / "
        "explicit end, and decides RETAIN or DISCARD by versioned policy."
    ),
    lifespan=lifespan,
)

app.include_router(ingest.router)
app.include_router(traces.router)
app.include_router(policies.router)


@app.get("/health", tags=["meta"])
async def health():
    return {"status": "ok"}


@app.get("/ready", tags=["meta"])
async def ready():
    await ping()
    return {"status": "ready"}


@app.post("/admin/sweep", tags=["meta"])
async def trigger_sweep():
    """Run one close/watermark sweep immediately (handy for demos/tests)."""
    async with SessionLocal() as session:
        return await sweep_once(session)


# ---------------------------------------------------------------------------
# Test-only fault-injection hooks. Disabled unless TS_ENABLE_TEST_HOOKS=true
# (the shipped Docker image/compose never sets it), so the concurrency tests
# can slow the *business transaction* inside a black-box server process and
# assert that same-key/different-payload peers do not wait for it.
# ---------------------------------------------------------------------------

_test_hooks_state = {"ingest_calls": 0, "publish_calls": 0}
_test_hooks_originals = {}


if settings.enable_test_hooks:

    @app.post("/admin/test/slow", tags=["meta-test"])
    async def enable_slow_hooks(delay_ms: int = 1000):
        import asyncio

        from app.routers import ingest as ingest_router
        from app.routers import policies as policies_router

        _test_hooks_state["ingest_calls"] = 0
        _test_hooks_state["publish_calls"] = 0

        if "ingest" not in _test_hooks_originals:
            original_ingest = ingest_router.ingest_spans
            _test_hooks_originals["ingest"] = original_ingest

            async def slow_ingest(session, trace_id, spans):
                _test_hooks_state["ingest_calls"] += 1
                # Keep the business transaction open (claim row lock held) for
                # a controllable while.
                await asyncio.sleep(delay_ms / 1000.0)
                return await original_ingest(session, trace_id, spans)

            ingest_router.ingest_spans = slow_ingest

        if "publish" not in _test_hooks_originals:
            original_publish = policies_router.publish_policy
            _test_hooks_originals["publish"] = original_publish

            async def slow_publish(session, body):
                _test_hooks_state["publish_calls"] += 1
                await asyncio.sleep(delay_ms / 1000.0)
                return await original_publish(session, body)

            policies_router.publish_policy = slow_publish

        return {"status": "enabled", "delay_ms": delay_ms}

    @app.post("/admin/test/reset", tags=["meta-test"])
    async def reset_slow_hooks():
        from app.routers import ingest as ingest_router
        from app.routers import policies as policies_router

        if "ingest" in _test_hooks_originals:
            ingest_router.ingest_spans = _test_hooks_originals.pop("ingest")
        if "publish" in _test_hooks_originals:
            policies_router.publish_policy = _test_hooks_originals.pop("publish")
        return {"status": "reset"}

    @app.get("/admin/test/state", tags=["meta-test"])
    async def slow_hooks_state():
        return dict(_test_hooks_state)
