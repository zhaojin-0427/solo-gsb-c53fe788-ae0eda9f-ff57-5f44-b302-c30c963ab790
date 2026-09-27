"""API + service integration tests against a real PostgreSQL.

Run with the database reachable (e.g. inside docker-compose):
    docker compose exec api pytest -q tests/test_integration.py
Skipped automatically if TS_DATABASE_URL is unreachable.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import httpx
import pytest
import pytest_asyncio

from app.config import get_settings
from app.db import SessionLocal, ensure_schema, engine
from app.models import CloseReason, Trace, TraceState
from app.services.traces import finalize_trace, sweep_once

settings = get_settings()

pytestmark = pytest.mark.integration


def _iso(dt: datetime) -> str:
    return dt.isoformat()


T0 = datetime(2026, 9, 21, 12, 0, 0, tzinfo=timezone.utc)


def span_payload(span_id, parent_id=None, status="OK", duration_ms=50,
                 name="op", attributes=None, links=None, start=T0):
    return {
        "span_id": span_id,
        "parent_id": parent_id,
        "name": name,
        "start_time": _iso(start),
        "duration_ms": duration_ms,
        "status": status,
        "attributes": attributes or {},
        "links": links or [],
    }


@pytest_asyncio.fixture(scope="module", autouse=True)
async def db_ready():
    try:
        await ensure_schema(engine)
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    # Clean tables before module runs, then seed the default policy.
    from sqlalchemy import text

    from app.models import Policy
    from app.services.default_policy import DEFAULT_POLICY_NAME, DEFAULT_POLICY_SPEC

    async with SessionLocal() as session:
        await session.execute(
            text(
                "TRUNCATE traces, spans, decisions, policies, "
                "idempotency_keys RESTART IDENTITY CASCADE"
            )
        )
        session.add(
            Policy(
                name=DEFAULT_POLICY_NAME, spec=DEFAULT_POLICY_SPEC, is_active=True
            )
        )
        await session.commit()
    yield


@pytest_asyncio.fixture
async def client():
    from app.main import app

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


# ---------------------------------------------------------------------------
# Real-HTTP concurrency tests: an actual uvicorn server on a TCP port, requests
# fired in parallel from OS threads. Unlike the in-process ASGITransport these
# exercise genuine socket-level concurrency against real PostgreSQL rows/locks.
# ---------------------------------------------------------------------------


def _enable_slow_hooks(base_url, delay_ms: int):
    import httpx as _httpx

    r = _httpx.post(f"{base_url}/admin/test/slow?delay_ms={delay_ms}", timeout=10)
    assert r.status_code == 200, r.text


def _reset_slow_hooks(base_url):
    import httpx as _httpx

    _httpx.post(f"{base_url}/admin/test/reset", timeout=10)


def _hook_calls(base_url) -> dict:
    import httpx as _httpx

    return _httpx.get(f"{base_url}/admin/test/state", timeout=10).json()


@pytest.fixture(scope="module")
def http_server():
    """Start a real uvicorn server (separate process) on an ephemeral TCP port.

    A subprocess -- rather than an in-thread server -- keeps its asyncio loop
    and asyncpg connection pool completely independent from the test loop.
    """
    import os
    import socket
    import subprocess
    import sys
    import time

    import httpx as _httpx

    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]

    env = dict(os.environ)
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    env["PYTHONPATH"] = repo_root + os.pathsep + env.get("PYTHONPATH", "")
    env["TS_ENABLE_TEST_HOOKS"] = "true"
    proc = subprocess.Popen(
        [
            sys.executable, "-m", "uvicorn", "app.main:app",
            "--host", "127.0.0.1", "--port", str(port),
            "--log-level", "warning",
        ],
        cwd=repo_root,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        deadline = time.time() + 30
        while time.time() < deadline:
            if proc.poll() is not None:  # pragma: no cover
                raise RuntimeError("test uvicorn server exited early")
            try:
                if (
                    _httpx.get(f"http://127.0.0.1:{port}/health", timeout=1).status_code
                    == 200
                ):
                    break
            except Exception:
                time.sleep(0.15)
        else:  # pragma: no cover
            raise RuntimeError("test uvicorn server did not become ready")

        yield f"http://127.0.0.1:{port}"
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:  # pragma: no cover
            proc.kill()


def _post_concurrent(base_url, posts, timeout=15.0):
    """Fire (path, json, headers) POSTs in parallel OS threads over real HTTP.

    Returns a list aligned with ``posts`` of (elapsed_seconds, response).
    """
    import concurrent.futures
    import time

    import httpx as _httpx

    def one(item):
        path, payload, headers = item
        started = time.monotonic()
        kwargs = {"headers": headers, "timeout": timeout}
        if payload is not None:
            kwargs["json"] = payload
        try:
            r = _httpx.post(base_url + path, **kwargs)
        except Exception as exc:  # pragma: no cover - test failure surface
            return time.monotonic() - started, exc
        return time.monotonic() - started, r

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(posts)) as pool:
        return list(pool.map(one, posts))


async def _post_spans(client, trace_id, spans, headers=None):
    return await client.post(
        f"/v1/traces/{trace_id}/spans", json={"spans": spans}, headers=headers
    )


# ---------------------------------------------------------------------------


async def test_health(client):
    r = await client.get("/health")
    assert r.status_code == 200


async def test_explicit_end_clean_trace_discards(client):
    r = await _post_spans(client, "tr-clean", [
        span_payload("root"),
        span_payload("c1", "root"),
    ])
    assert r.status_code == 201
    assert r.json()["revision"] == 1

    r = await client.post("/v1/traces/tr-clean/end")
    assert r.status_code == 201
    body = r.json()
    assert body["decision"]["verdict"] == "DISCARD"
    assert body["decision"]["close_reason"] == "END_SIGNAL"
    assert body["trace"]["state"] == "ARCHIVED"
    assert body["decision"]["evidence"]["matched_rule"] is None


async def test_error_trace_retains_and_explains(client):
    # Out of order: child before parent.
    r = await _post_spans(client, "tr-err", [span_payload("leaf", "mid", status="ERROR")])
    assert r.status_code == 201
    r = await _post_spans(client, "tr-err", [
        span_payload("mid", "root"), span_payload("root")
    ])
    assert r.json()["stored"] == 2

    r = await client.post("/v1/traces/tr-err/end")
    decision = r.json()["decision"]
    assert decision["verdict"] == "RETAIN"
    assert decision["evidence"]["matched_rule"]["rule_id"] == "error-any"
    assert set(decision["evidence"]["effective_error_span_ids"]) == {"root", "mid", "leaf"}

    r = await client.get("/v1/traces/tr-err/decision")
    assert r.json()["evidence"]["verdict"] == "RETAIN"
    assert "explanation" in r.json()["evidence"]


async def test_idempotent_replay_same_key_same_payload(client):
    key = {"Idempotency-Key": "idem-1"}
    payload = {"spans": [span_payload("root")]}
    r1 = await client.post("/v1/traces/tr-idem/spans", json=payload, headers=key)
    r2 = await client.post("/v1/traces/tr-idem/spans", json=payload, headers=key)
    assert r1.status_code == r2.status_code == 201
    assert r1.json()["stored"] == 1
    # Replayed response, span not stored twice.
    assert r2.json()["results"][0]["outcome"] == "stored"
    detail = await client.get("/v1/traces/tr-idem")
    assert len(detail.json()["spans"]) == 1


async def test_idempotent_replay_different_payload_conflicts(client):
    key = {"Idempotency-Key": "idem-2"}
    await client.post(
        "/v1/traces/tr-idem-conf/spans",
        json={"spans": [span_payload("root")]},
        headers=key,
    )
    r = await client.post(
        "/v1/traces/tr-idem-conf/spans",
        json={"spans": [span_payload("root", duration_ms=999)]},
        headers=key,
    )
    assert r.status_code == 409
    assert r.json()["detail"]["error"] == "IDEMPOTENCY_CONFLICT"


async def test_identical_span_rewrite_is_duplicate(client):
    await _post_spans(client, "tr-dup", [span_payload("root")])
    r = await _post_spans(client, "tr-dup", [span_payload("root")])
    assert r.status_code == 200
    assert r.json()["duplicates"] == 1


async def test_conflicting_span_rewrite_rejected(client):
    await _post_spans(client, "tr-conflict", [span_payload("root")])
    r = await _post_spans(
        client, "tr-conflict", [span_payload("root", attributes={"x": 1})]
    )
    assert r.status_code == 409
    assert r.json()["detail"]["error"] == "SPAN_CONFLICT"


async def test_late_spans_create_new_revision_and_preserve_decision(client):
    await _post_spans(client, "tr-late", [span_payload("root")])
    r1 = await client.post("/v1/traces/tr-late/end")
    assert r1.json()["decision"]["revision"] == 1
    assert r1.json()["decision"]["verdict"] == "DISCARD"

    # Late span arrives after publication.
    r = await _post_spans(
        client, "tr-late", [span_payload("late", "root", status="ERROR")]
    )
    assert r.status_code == 201
    assert r.json()["revision"] == 2
    assert r.json()["state"] == "OPEN"

    # Published revision 1 decision is immutable.
    d1 = (await client.get("/v1/traces/tr-late/decision?revision=1")).json()
    assert d1["verdict"] == "DISCARD"
    assert d1["span_count"] == 1

    # Closing revision 2 yields a new decision.
    r2 = await client.post("/v1/traces/tr-late/end")
    assert r2.json()["decision"]["revision"] == 2
    assert r2.json()["decision"]["verdict"] == "RETAIN"

    revisions = (await client.get("/v1/traces/tr-late/revisions")).json()
    assert [d["revision"] for d in revisions] == [1, 2]


async def test_concurrent_finalization_archives_once(client):
    import asyncio

    from app.main import app

    await _post_spans(client, "tr-race", [span_payload("root")])
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as c:
        results = await asyncio.gather(
            *[c.post("/v1/traces/tr-race/end") for _ in range(5)]
        )
    statuses = sorted(r.status_code for r in results)
    # Exactly one "created" (201); the rest replay the published decision.
    assert statuses.count(201) == 1
    decisions = (await client.get("/v1/traces/tr-race/revisions")).json()
    assert len(decisions) == 1


async def test_policy_cas_conflict_and_publish(client):
    active = (await client.get("/v1/policies/active")).json()
    version = active["version"]

    bad = await client.post(
        "/v1/policies",
        json={
            "name": "stale",
            "expected_version": version + 999,
            "spec": {"default_decision": "DISCARD", "rules": []},
        },
    )
    assert bad.status_code == 409
    assert bad.json()["detail"]["error"] == "CAS_CONFLICT"

    ok = await client.post(
        "/v1/policies",
        json={
            "name": "retain-all",
            "expected_version": version,
            "spec": {"default_decision": "RETAIN", "rules": []},
        },
    )
    assert ok.status_code == 201
    new_version = ok.json()["version"]
    assert new_version == version + 1

    # Restoring default-ish policy for subsequent tests.
    await client.post(
        "/v1/policies",
        json={
            "name": "default-restored",
            "expected_version": new_version,
            "spec": {
                "default_decision": "DISCARD",
                "propagation": {"error_to_parents": True, "error_link_marks": "source"},
                "rules": [
                    {"id": "error-any", "scope": "any", "priority": 200,
                     "decision": "RETAIN", "match": {"status": "ERROR"}},
                    {"id": "slow-any", "scope": "any", "priority": 100,
                     "decision": "RETAIN", "match": {"min_duration_ms": 1000}},
                ],
            },
        },
    )


async def test_revision_replay_with_different_policy(client):
    await _post_spans(client, "tr-replay", [span_payload("root", duration_ms=100)])
    await client.post("/v1/traces/tr-replay/end")

    # Publish an always-retain policy, replay revision 1 with it (read-only).
    active = (await client.get("/v1/policies/active")).json()
    created = await client.post(
        "/v1/policies",
        json={
            "name": "retain-all-tmp",
            "expected_version": active["version"],
            "spec": {"default_decision": "RETAIN", "rules": []},
        },
    )
    retain_version = created.json()["version"]

    r = await client.post(
        f"/v1/traces/tr-replay/revisions/1/replay?policy_version={retain_version}"
    )
    assert r.status_code == 200
    body = r.json()
    assert body["verdict"] == "RETAIN"
    assert body["stored_verdict"] == "DISCARD"
    assert body["matches_published"] is False

    # Stored decision untouched.
    stored = (await client.get("/v1/traces/tr-replay/decision")).json()
    assert stored["verdict"] == "DISCARD"


async def test_idle_window_close_and_hard_watermark(client):
    # Window close via explicit sweep on a trace idle beyond window.
    await _post_spans(client, "tr-window", [
        span_payload("root", duration_ms=10),
        span_payload("c", "root", status="ERROR"),
    ])
    async with SessionLocal() as session:
        trace = await session.get(Trace, "tr-window")
        trace.last_span_at = trace.last_span_at - timedelta(
            seconds=settings.window_seconds + 5
        )
        await session.commit()

    async with SessionLocal() as session:
        result = await sweep_once(session)
    assert result["window_closed"] >= 1
    decision = (await client.get("/v1/traces/tr-window/decision")).json()
    assert decision["close_reason"] == "WINDOW"
    assert decision["verdict"] == "RETAIN"

    # Hard watermark: an unclosed trace becomes INDETERMINATE.
    await _post_spans(client, "tr-wm", [span_payload("root")])
    async with SessionLocal() as session:
        trace = await session.get(Trace, "tr-wm")
        trace.revision_started_at = trace.revision_started_at - timedelta(
            seconds=settings.hard_watermark_seconds + 5
        )
        # Keep it "fresh" so the idle window does not fire first.
        trace.last_span_at = trace.last_span_at + timedelta(seconds=1)
        await session.commit()

    async with SessionLocal() as session:
        result = await sweep_once(session)
    assert result["watermarked"] >= 1
    decision = (await client.get("/v1/traces/tr-wm/decision")).json()
    assert decision["verdict"] == "INDETERMINATE"
    assert decision["close_reason"] == "WATERMARK"
    assert "would_have_verdict" in decision["evidence"]


async def test_trace_not_found(client):
    assert (await client.get("/v1/traces/does-not-exist")).status_code == 404
    assert (await client.post("/v1/traces/does-not-exist/end")).status_code == 404


async def test_batch_duplicate_span_id_rejected(client):
    r = await client.post(
        "/v1/traces/tr-batch-dup/spans",
        json={"spans": [span_payload("a"), span_payload("a")]},
    )
    assert r.status_code == 422


async def test_timing_requires_end_or_duration(client):
    r = await client.post(
        "/v1/traces/tr-bad-time/spans",
        json={"spans": [{"span_id": "a", "start_time": _iso(T0)}]},
    )
    assert r.status_code == 422


# ---------------------------------------------------------------------------
# Idempotency under real concurrency (atomic PENDING placeholder protocol)
# ---------------------------------------------------------------------------


async def test_concurrent_same_key_same_payload_runs_business_once(
    client, monkeypatch
):
    """N concurrent first calls with the same key+payload: the business
    write executes exactly once; every request gets the identical 201."""
    import asyncio

    from app.main import app
    from app.routers import ingest as ingest_router

    key = {"Idempotency-Key": "concurrent-once"}
    payload = {"spans": [span_payload("root")]}
    calls = 0

    original = ingest_router.ingest_spans

    async def counting_ingest(session, trace_id, spans):
        nonlocal calls
        calls += 1
        # Widen the race window: followers must be blocked on the PENDING
        # placeholder while the leader is still inside the transaction.
        await asyncio.sleep(0.4)
        return await original(session, trace_id, spans)

    monkeypatch.setattr(ingest_router, "ingest_spans", counting_ingest)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as c:
        results = await asyncio.gather(
            *[
                c.post("/v1/traces/tr-concurrent-once/spans",
                       json=payload, headers=key)
                for _ in range(8)
            ]
        )

    assert calls == 1, "ingest business logic ran more than once"
    assert [r.status_code for r in results] == [201] * 8
    bodies = [r.json() for r in results]
    assert all(b == bodies[0] for b in bodies)
    replay_flags = [r.headers.get("x-idempotent-replay") for r in results]
    assert replay_flags.count("true") == 7

    detail = await client.get("/v1/traces/tr-concurrent-once")
    assert len(detail.json()["spans"]) == 1


async def test_concurrent_same_key_different_payload_conflicts(client):
    """Same key, different payloads racing: one wins and stores; the other
    gets 409 and never executes its business write."""
    import asyncio

    from app.main import app

    key = {"Idempotency-Key": "concurrent-conflict"}
    payload_a = {"spans": [span_payload("a")]}
    payload_b = {"spans": [span_payload("b", duration_ms=999)]}

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as c:
        ra, rb = await asyncio.gather(
            c.post("/v1/traces/tr-concurrent-conf/spans",
                   json=payload_a, headers=key),
            c.post("/v1/traces/tr-concurrent-conf/spans",
                   json=payload_b, headers=key),
        )

    statuses = {ra.status_code, rb.status_code}
    assert statuses == {201, 409}
    conflict = rb if rb.status_code == 409 else ra
    assert conflict.json()["detail"]["error"] == "IDEMPOTENCY_CONFLICT"

    # Exactly one of the two payloads was ever written.
    detail = (await client.get("/v1/traces/tr-concurrent-conf")).json()
    assert len(detail["spans"]) == 1
    assert detail["spans"][0]["span_id"] in {"a", "b"}

    # Any later reuse of the key with the *losing* payload conflicts
    # immediately (whichever side won the race).
    loser_payload = payload_b if detail["spans"][0]["span_id"] == "a" else payload_a
    r = await client.post(
        "/v1/traces/tr-concurrent-conf/spans",
        json=loser_payload,
        headers=key,
    )
    assert r.status_code == 409


async def test_business_failure_rolls_back_placeholder_and_key_reusable(client):
    """When the leader's business logic fails (409 SPAN_CONFLICT), the
    placeholder is rolled back; concurrent followers retry (not 500) and the
    key stays usable because no response was ever published."""
    import asyncio

    from app.main import app

    # Pre-existing span "root".
    await _post_spans(client, "tr-idem-retry", [span_payload("root")])

    key = {"Idempotency-Key": "concurrent-failing"}
    bad_payload = {"spans": [span_payload("root", attributes={"x": 1})]}

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as c:
        results = await asyncio.gather(
            *[
                c.post("/v1/traces/tr-idem-retry/spans",
                       json=bad_payload, headers=key)
                for _ in range(5)
            ]
        )
    assert {r.status_code for r in results} == {409}
    assert all(
        r.json()["detail"]["error"] == "SPAN_CONFLICT" for r in results
    )

    # No placeholder survived; the unbound key can be used by the corrected
    # (controlled retry) request.
    ok_payload = {"spans": [span_payload("child", "root")]}
    r = await client.post(
        "/v1/traces/tr-idem-retry/spans", json=ok_payload, headers=key
    )
    assert r.status_code == 201
    assert r.json()["stored"] == 1

    # Same key + same corrected payload replays.
    r2 = await client.post(
        "/v1/traces/tr-idem-retry/spans", json=ok_payload, headers=key
    )
    assert r2.status_code == 201
    assert r2.headers.get("x-idempotent-replay") == "true"
    assert r2.json() == r.json()


async def test_stale_pending_placeholder_is_taken_over(client):
    """A PENDING placeholder older than the TTL (dead owner) is taken over
    exactly once and the request completes normally."""
    from app.models import IdempotencyKey, utcnow
    from app.services.idempotency import hash_payload

    payload = {"spans": [span_payload("root")]}
    scope = "ingest:tr-stale-pending"
    request_hash = hash_payload(scope, payload)

    async with SessionLocal() as session:
        session.add(
            IdempotencyKey(
                scope=scope,
                key="k-stale",
                request_hash=request_hash,
                state="PENDING",
                attempts=1,
                locked_at=utcnow() - timedelta(minutes=10),
            )
        )
        await session.commit()

    r = await client.post(
        "/v1/traces/tr-stale-pending/spans", json=payload,
        headers={"Idempotency-Key": "k-stale"},
    )
    assert r.status_code == 201

    # Follower replays the taken-over result; takeover bumped attempts.
    r2 = await client.post(
        "/v1/traces/tr-stale-pending/spans", json=payload,
        headers={"Idempotency-Key": "k-stale"},
    )
    assert r2.status_code == 201
    assert r2.headers.get("x-idempotent-replay") == "true"

    async with SessionLocal() as session:
        from sqlalchemy import select

        row = (
            await session.execute(
                select(IdempotencyKey).where(
                    IdempotencyKey.scope == scope,
                    IdempotencyKey.key == "k-stale",
                )
            )
        ).scalar_one()
        assert row.state == "COMPLETED"
        assert row.attempts == 2


async def test_concurrent_end_with_key_replays_single_decision(client):
    """Concurrent idempotent /end calls: one leader finalizes, all others
    block and replay the identical 201; still one decision only."""
    import asyncio

    from app.main import app

    await _post_spans(client, "tr-concurrent-end", [span_payload("root")])
    key = {"Idempotency-Key": "end-once"}

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as c:
        results = await asyncio.gather(
            *[
                c.post("/v1/traces/tr-concurrent-end/end", headers=key)
                for _ in range(5)
            ]
        )

    assert [r.status_code for r in results] == [201] * 5
    bodies = [r.json() for r in results]
    assert all(b == bodies[0] for b in bodies)
    assert bodies[0]["note"] is None
    decisions = (await client.get("/v1/traces/tr-concurrent-end/revisions")).json()
    assert len(decisions) == 1


async def test_concurrent_policy_publish_with_key_publishes_once(client):
    """Concurrent CAS publishes sharing a key produce a single new version;
    followers replay it instead of colliding on CAS."""
    import asyncio

    from app.main import app

    active = (await client.get("/v1/policies/active")).json()
    key = {"Idempotency-Key": "publish-once"}
    body = {
        "name": "idem-published",
        "expected_version": active["version"],
        "spec": {"default_decision": "DISCARD", "rules": []},
    }

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as c:
        results = await asyncio.gather(
            *[c.post("/v1/policies", json=body, headers=key) for _ in range(4)]
        )

    assert [r.status_code for r in results] == [201] * 4
    versions = {r.json()["version"] for r in results}
    assert len(versions) == 1

    # Sequential same-key same-body replays; different body conflicts 409.
    replay = await client.post("/v1/policies", json=body, headers=key)
    assert replay.status_code == 201
    assert replay.headers.get("x-idempotent-replay") == "true"
    assert replay.json()["version"] == versions.pop()

    conflict = await client.post(
        "/v1/policies",
        json={**body, "name": "idem-published-other"},
        headers=key,
    )
    assert conflict.status_code == 409
    assert conflict.json()["detail"]["error"] == "IDEMPOTENCY_CONFLICT"


# ---------------------------------------------------------------------------
# Stale-placeholder takeover vs. completing leader (controlled interleaving)
#
# Regression tests for the expired-placeholder takeover race: the leader's
# business outlives the PENDING TTL, a same-key peer reads the PENDING row,
# and the leader commits COMPLETED *before* the peer's takeover probe runs.
# The probe must re-read the authoritative row under its FOR UPDATE lock
# instead of reusing the ORM session's cached object -- otherwise the
# "reclaim" rewrites the completed row's fingerprint while keeping its
# response, and a same-key/different-payload request is wrongly replayed as
# the first writer's result instead of getting 409.
# ---------------------------------------------------------------------------


def _hook_takeover_interleaving(monkeypatch, scope, router_module, attr):
    """Deterministically reproduce the takeover/complete interleaving.

    * the entry point's business (``router_module.attr``) is gated so the
      leader cannot commit before the follower classified the placeholder;
    * the follower's unlocked phase-1 read of the PENDING row parks until
      the test releases it -- i.e. its takeover probe (SKIP LOCKED) only
      runs after the leader committed COMPLETED.

    Returns (business_entered, placeholder_seen, release_takeover, calls).
    """
    import asyncio

    from app.services import idempotency

    business_entered = asyncio.Event()
    placeholder_seen = asyncio.Event()
    release_takeover = asyncio.Event()
    calls = {"n": 0}

    original_business = getattr(router_module, attr)

    async def gated_business(*args, **kwargs):
        calls["n"] += 1
        business_entered.set()
        # Hold the business transaction open until the follower has read the
        # PENDING placeholder, so the follower classifies while we are PENDING.
        await placeholder_seen.wait()
        return await original_business(*args, **kwargs)

    monkeypatch.setattr(router_module, attr, gated_business)

    original_read = idempotency._read
    parked = {"done": False}

    async def parked_read(session, scope_, key_):
        row = await original_read(session, scope_, key_)
        if (
            not parked["done"]
            and scope_ == scope
            and row is not None
            and row.state == "PENDING"
        ):
            parked["done"] = True
            placeholder_seen.set()
            # Park between the unlocked read and the takeover probe; the
            # leader commits COMPLETED in between.
            await release_takeover.wait()
        return row

    monkeypatch.setattr(idempotency, "_read", parked_read)
    return business_entered, placeholder_seen, release_takeover, calls


async def _fetch_idempotency_row(scope, key):
    from sqlalchemy import select

    from app.models import IdempotencyKey

    async with SessionLocal() as session:
        return (
            await session.execute(
                select(IdempotencyKey).where(
                    IdempotencyKey.scope == scope,
                    IdempotencyKey.key == key,
                )
            )
        ).scalar_one()


async def test_takeover_race_completed_row_conflicts_different_payload(
    client, monkeypatch
):
    """Leader's business outlives the PENDING TTL; a same-key/
    different-payload request reads the PENDING row and probes the takeover
    only after the leader committed. The completed row keeps the first
    writer's fingerprint/status/body and the late request gets 409 -- it
    must never be replayed as the first writer's result."""
    import asyncio

    from app.main import app
    from app.routers import ingest as ingest_router
    from app.schemas import BatchIngestIn
    from app.services import idempotency
    from app.services.idempotency import hash_payload

    # The leader's business "exceeds" the TTL by construction.
    monkeypatch.setattr(idempotency.settings, "idempotency_pending_ttl_ms", 0)

    trace = "tr-takeover-race"
    scope = f"ingest:{trace}"
    key = {"Idempotency-Key": "takeover-race-key"}
    payload_a = {"spans": [span_payload("a")]}
    payload_b = {"spans": [span_payload("b", duration_ms=999)]}

    business_entered, placeholder_seen, release_takeover, calls = (
        _hook_takeover_interleaving(monkeypatch, scope, ingest_router, "ingest_spans")
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as c:
        leader = asyncio.create_task(
            c.post(f"/v1/traces/{trace}/spans", json=payload_a, headers=key)
        )
        await asyncio.wait_for(business_entered.wait(), 5)
        follower = asyncio.create_task(
            c.post(f"/v1/traces/{trace}/spans", json=payload_b, headers=key)
        )
        # The follower has now read the PENDING placeholder and parked; let
        # the leader's business finish and commit COMPLETED, then release
        # the follower's takeover probe.
        await asyncio.wait_for(placeholder_seen.wait(), 5)
        leader_resp = await asyncio.wait_for(leader, 5)
        release_takeover.set()
        follower_resp = await asyncio.wait_for(follower, 5)

    assert leader_resp.status_code == 201
    assert follower_resp.status_code == 409
    assert follower_resp.json()["detail"]["error"] == "IDEMPOTENCY_CONFLICT"
    assert calls["n"] == 1, "different-payload request must never run the business"

    # Only the first writer's span was stored.
    detail = (await client.get(f"/v1/traces/{trace}")).json()
    assert [s["span_id"] for s in detail["spans"]] == ["a"]

    # Ledger consistency: the completed row kept the first writer's
    # fingerprint, status code and response body; no takeover happened.
    row = await _fetch_idempotency_row(scope, "takeover-race-key")
    assert row.state == "COMPLETED"
    assert row.request_hash == hash_payload(
        scope, BatchIngestIn(**payload_a).model_dump(mode="json")
    )
    assert row.status_code == 201
    assert row.response_body["stored"] == 1
    assert row.response_body["results"] == [{"span_id": "a", "outcome": "stored"}]
    assert row.attempts == 1

    # Same key + original payload replays the first writer's exact response.
    replay = await client.post(f"/v1/traces/{trace}/spans", json=payload_a, headers=key)
    assert replay.status_code == 201
    assert replay.headers.get("x-idempotent-replay") == "true"
    assert replay.json() == leader_resp.json()

    # The conflicting payload still conflicts on later reuse.
    again = await client.post(f"/v1/traces/{trace}/spans", json=payload_b, headers=key)
    assert again.status_code == 409


async def test_takeover_race_same_payload_replays_without_reexecute(
    client, monkeypatch
):
    """Same interleaving, but the follower carries the *same* payload: it
    must replay the leader's identical response, the business runs exactly
    once, and the ledger shows no takeover (attempts stays 1)."""
    import asyncio

    from app.main import app
    from app.routers import ingest as ingest_router
    from app.services import idempotency

    monkeypatch.setattr(idempotency.settings, "idempotency_pending_ttl_ms", 0)

    trace = "tr-takeover-same"
    scope = f"ingest:{trace}"
    key = {"Idempotency-Key": "takeover-same-key"}
    payload = {"spans": [span_payload("root")]}

    business_entered, placeholder_seen, release_takeover, calls = (
        _hook_takeover_interleaving(monkeypatch, scope, ingest_router, "ingest_spans")
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as c:
        leader = asyncio.create_task(
            c.post(f"/v1/traces/{trace}/spans", json=payload, headers=key)
        )
        await asyncio.wait_for(business_entered.wait(), 5)
        follower = asyncio.create_task(
            c.post(f"/v1/traces/{trace}/spans", json=payload, headers=key)
        )
        await asyncio.wait_for(placeholder_seen.wait(), 5)
        leader_resp = await asyncio.wait_for(leader, 5)
        release_takeover.set()
        follower_resp = await asyncio.wait_for(follower, 5)

    assert leader_resp.status_code == 201
    assert follower_resp.status_code == 201
    assert follower_resp.headers.get("x-idempotent-replay") == "true"
    assert follower_resp.json() == leader_resp.json()
    assert calls["n"] == 1, "business must run exactly once"

    detail = (await client.get(f"/v1/traces/{trace}")).json()
    assert len(detail["spans"]) == 1

    row = await _fetch_idempotency_row(scope, "takeover-same-key")
    assert row.state == "COMPLETED"
    assert row.status_code == 201
    assert row.attempts == 1, "a completed row must never be taken over"


async def test_takeover_race_end_entry_replays_single_decision(client, monkeypatch):
    """The /end idempotent entry point under the same interleaving: the
    follower replays the leader's published decision; the completed ledger
    row is not taken over."""
    import asyncio

    from app.main import app
    from app.routers import ingest as ingest_router
    from app.services import idempotency

    monkeypatch.setattr(idempotency.settings, "idempotency_pending_ttl_ms", 0)

    trace = "tr-takeover-end"
    scope = f"end:{trace}"
    key = {"Idempotency-Key": "takeover-end-key"}
    await _post_spans(client, trace, [span_payload("root")])

    business_entered, placeholder_seen, release_takeover, calls = (
        _hook_takeover_interleaving(
            monkeypatch, scope, ingest_router, "finalize_trace"
        )
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as c:
        leader = asyncio.create_task(
            c.post(f"/v1/traces/{trace}/end", headers=key)
        )
        await asyncio.wait_for(business_entered.wait(), 5)
        follower = asyncio.create_task(
            c.post(f"/v1/traces/{trace}/end", headers=key)
        )
        await asyncio.wait_for(placeholder_seen.wait(), 5)
        leader_resp = await asyncio.wait_for(leader, 5)
        release_takeover.set()
        follower_resp = await asyncio.wait_for(follower, 5)

    assert leader_resp.status_code == 201
    assert follower_resp.status_code == 201
    assert follower_resp.headers.get("x-idempotent-replay") == "true"
    assert follower_resp.json() == leader_resp.json()
    assert calls["n"] == 1

    decisions = (await client.get(f"/v1/traces/{trace}/revisions")).json()
    assert len(decisions) == 1

    row = await _fetch_idempotency_row(scope, "takeover-end-key")
    assert row.state == "COMPLETED"
    assert row.status_code == 201
    assert row.attempts == 1


async def test_takeover_race_policy_publish_entry_conflicts(client, monkeypatch):
    """The policy-publish idempotent entry point: a different-payload
    publish sharing the in-flight key probes the takeover after the leader
    committed and must get 409; only the leader's policy is published."""
    import asyncio

    from app.main import app
    from app.routers import policies as policies_router
    from app.schemas import PublishPolicyIn
    from app.services import idempotency
    from app.services.idempotency import hash_payload

    monkeypatch.setattr(idempotency.settings, "idempotency_pending_ttl_ms", 0)

    scope = "policies:publish"
    key = {"Idempotency-Key": "takeover-publish-key"}
    active = (await client.get("/v1/policies/active")).json()
    body_a = {
        "name": "takeover-policy-a",
        "expected_version": active["version"],
        "spec": {"default_decision": "DISCARD", "rules": []},
    }
    body_b = {**body_a, "name": "takeover-policy-b"}

    business_entered, placeholder_seen, release_takeover, calls = (
        _hook_takeover_interleaving(
            monkeypatch, scope, policies_router, "publish_policy"
        )
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as c:
        leader = asyncio.create_task(
            c.post("/v1/policies", json=body_a, headers=key)
        )
        await asyncio.wait_for(business_entered.wait(), 5)
        follower = asyncio.create_task(
            c.post("/v1/policies", json=body_b, headers=key)
        )
        await asyncio.wait_for(placeholder_seen.wait(), 5)
        leader_resp = await asyncio.wait_for(leader, 5)
        release_takeover.set()
        follower_resp = await asyncio.wait_for(follower, 5)

    assert leader_resp.status_code == 201
    assert leader_resp.json()["name"] == "takeover-policy-a"
    assert follower_resp.status_code == 409
    assert follower_resp.json()["detail"]["error"] == "IDEMPOTENCY_CONFLICT"
    assert calls["n"] == 1

    row = await _fetch_idempotency_row(scope, "takeover-publish-key")
    assert row.state == "COMPLETED"
    assert row.request_hash == hash_payload(
        scope, PublishPolicyIn(**body_a).model_dump(mode="json")
    )
    assert row.status_code == 201
    assert row.attempts == 1

    # Same key + original body replays the published policy; the conflicting
    # body still conflicts.
    replay = await client.post("/v1/policies", json=body_a, headers=key)
    assert replay.status_code == 201
    assert replay.headers.get("x-idempotent-replay") == "true"
    assert replay.json() == leader_resp.json()
    again = await client.post("/v1/policies", json=body_b, headers=key)
    assert again.status_code == 409


async def test_failed_reelection_loop_terminates_and_replays(client, monkeypatch):
    """FAILED re-election under contention: N peers read the same FAILED row
    simultaneously (gated); exactly one wins the election and runs the
    business, the losers loop, re-classify from the authoritative row (they
    must not spin on a cached FAILED snapshot) and replay the winner's
    identical response. The key stays consistent for the corrected payload.
    """
    import asyncio

    from app.main import app
    from app.models import IdempotencyKey, utcnow
    from app.routers import ingest as ingest_router
    from app.schemas import BatchIngestIn
    from app.services import idempotency
    from app.services.idempotency import hash_payload

    trace = "tr-reelection"
    scope = f"ingest:{trace}"
    key = {"Idempotency-Key": "reelection-key"}
    payload = {"spans": [span_payload("root")]}
    n = 4

    # Seed the FAILED claim of a previously failed attempt (fingerprint of
    # the old, bad payload -- a corrected retry may take the key over).
    async with SessionLocal() as session:
        session.add(
            IdempotencyKey(
                scope=scope,
                key="reelection-key",
                request_hash=hash_payload(scope, {"spans": [{"span_id": "old"}]}),
                state="FAILED",
                attempts=1,
                locked_at=utcnow(),
            )
        )
        await session.commit()

    # Gate: all N peers classify the FAILED row before anyone's election
    # UPDATE, so every one of them contests the same election.
    original_read = idempotency._read
    seen = {"n": 0}
    gate = asyncio.Event()

    async def barrier_read(session_, scope_, key_):
        row = await original_read(session_, scope_, key_)
        if (
            scope_ == scope
            and row is not None
            and row.state == "FAILED"
            and seen["n"] < n
        ):
            seen["n"] += 1
            if seen["n"] == n:
                gate.set()
            await gate.wait()
        return row

    monkeypatch.setattr(idempotency, "_read", barrier_read)

    calls = {"n": 0}
    original_ingest = ingest_router.ingest_spans

    async def counting_ingest(*args, **kwargs):
        calls["n"] += 1
        return await original_ingest(*args, **kwargs)

    monkeypatch.setattr(ingest_router, "ingest_spans", counting_ingest)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as c:
        results = await asyncio.wait_for(
            asyncio.gather(
                *[
                    c.post(f"/v1/traces/{trace}/spans", json=payload, headers=key)
                    for _ in range(n)
                ]
            ),
            15,
        )

    assert [r.status_code for r in results] == [201] * n
    bodies = [r.json() for r in results]
    assert all(b == bodies[0] for b in bodies)
    replay_flags = [r.headers.get("x-idempotent-replay") for r in results]
    assert replay_flags.count("true") == n - 1
    assert calls["n"] == 1, "exactly one re-elected leader may run the business"

    # Ledger: the retry leader completed the key with the corrected
    # payload's fingerprint; re-election is not a crash takeover (attempts
    # stays 1).
    row = await _fetch_idempotency_row(scope, "reelection-key")
    assert row.state == "COMPLETED"
    assert row.request_hash == hash_payload(
        scope, BatchIngestIn(**payload).model_dump(mode="json")
    )
    assert row.status_code == 201
    assert row.attempts == 1

    # The key now replays the corrected payload and conflicts everything else.
    replay = await client.post(f"/v1/traces/{trace}/spans", json=payload, headers=key)
    assert replay.status_code == 201
    assert replay.headers.get("x-idempotent-replay") == "true"
    conflict = await client.post(
        f"/v1/traces/{trace}/spans",
        json={"spans": [span_payload("other", duration_ms=999)]},
        headers=key,
    )
    assert conflict.status_code == 409


# ---------------------------------------------------------------------------
# Placeholder reclaim vs. live business lock (controlled interleaving)
#
# Regression tests for the stale-fingerprint misjudgement: a same-key request
# reads an EXPIRED PENDING placeholder carrying the old fingerprint; before
# its SKIP LOCKED probe runs, a peer reclaims the placeholder with a new
# fingerprint, commits the claim, and takes the business row lock. The first
# request's probe then comes back empty (lock held by the new owner) and the
# same/different-payload decision must be re-judged against the CURRENT
# committed owner's fingerprint -- never the stale pre-takeover one --
# otherwise a request carrying the owner's exact payload is wrongly refused
# 409 IDEMPOTENCY_CONFLICT.
# ---------------------------------------------------------------------------


def _hook_reclaim_interleaving(monkeypatch, scope, router_module, attr,
                               business_side_effect=None):
    """Deterministically reproduce the reclaim/business-lock interleaving.

    * the first phase-1 read of the scope's PENDING row (the follower's read
      of the expired placeholder with the old fingerprint) parks until the
      test releases it -- i.e. the follower's SKIP LOCKED probe only runs
      after the reclaimer committed its claim AND took the business lock;
    * the entry point's business (``router_module.attr``) is gated so the
      reclaimer holds the business row lock until the test releases it;
      ``business_side_effect`` (if given) is raised by the first call.

    Returns (placeholder_seen, business_entered, release_probe,
    release_business, calls).
    """
    import asyncio

    from app.services import idempotency

    placeholder_seen = asyncio.Event()
    business_entered = asyncio.Event()
    release_probe = asyncio.Event()
    release_business = asyncio.Event()
    calls = {"n": 0}

    original_business = getattr(router_module, attr)

    async def gated_business(*args, **kwargs):
        calls["n"] += 1
        business_entered.set()
        # Hold the business transaction (and with it the claim row lock) open
        # until the follower has probed and classified against the new owner.
        await release_business.wait()
        if business_side_effect is not None and calls["n"] == 1:
            raise business_side_effect
        return await original_business(*args, **kwargs)

    monkeypatch.setattr(router_module, attr, gated_business)

    original_read = idempotency._read
    parked = {"done": False}

    async def parked_read(session, scope_, key_):
        row = await original_read(session, scope_, key_)
        if (
            not parked["done"]
            and scope_ == scope
            and row is not None
            and row.state == "PENDING"
        ):
            parked["done"] = True
            placeholder_seen.set()
            # Park between the unlocked read and the SKIP LOCKED probe; the
            # peer reclaims the placeholder (new fingerprint) and takes the
            # business row lock in between.
            await release_probe.wait()
        return row

    monkeypatch.setattr(idempotency, "_read", parked_read)
    return placeholder_seen, business_entered, release_probe, release_business, calls


def _seed_expired_pending(scope, key, old_fingerprint_payload):
    """Insert a crash-leftover PENDING row with an old (different) fingerprint."""
    from app.models import IdempotencyKey, utcnow
    from app.services.idempotency import hash_payload

    async def seed():
        async with SessionLocal() as session:
            session.add(
                IdempotencyKey(
                    scope=scope,
                    key=key,
                    request_hash=hash_payload(scope, old_fingerprint_payload),
                    state="PENDING",
                    attempts=1,
                    locked_at=utcnow() - timedelta(minutes=10),
                )
            )
            await session.commit()

    return seed()


async def test_reclaim_race_same_payload_waits_and_replays(client, monkeypatch):
    """A crash-leftover PENDING row (old fingerprint) is reclaimed by a peer
    with a new fingerprint while a same-key request carrying the SAME payload
    as the reclaimer is still classifying: its SKIP LOCKED probe loses to the
    new owner's business lock, and it must wait and replay the owner's exact
    response -- not be refused 409 on the stale fingerprint."""
    import asyncio

    from app.main import app
    from app.routers import ingest as ingest_router
    from app.schemas import BatchIngestIn
    from app.services.idempotency import hash_payload

    trace = "tr-reclaim-same"
    scope = f"ingest:{trace}"
    key = {"Idempotency-Key": "reclaim-same-key"}
    payload = {"spans": [span_payload("root")]}

    await _seed_expired_pending(scope, "reclaim-same-key",
                                {"spans": [{"span_id": "crashed-owner"}]})

    (placeholder_seen, business_entered, release_probe,
     release_business, calls) = _hook_reclaim_interleaving(
        monkeypatch, scope, ingest_router, "ingest_spans"
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as c:
        # The follower reads the expired placeholder (old fingerprint) and
        # parks before its takeover probe.
        follower = asyncio.create_task(
            c.post(f"/v1/traces/{trace}/spans", json=payload, headers=key)
        )
        await asyncio.wait_for(placeholder_seen.wait(), 5)
        # The reclaimer takes the placeholder over with the new fingerprint,
        # commits the claim and holds the business row lock.
        reclaimer = asyncio.create_task(
            c.post(f"/v1/traces/{trace}/spans", json=payload, headers=key)
        )
        await asyncio.wait_for(business_entered.wait(), 5)
        # The follower's probe now comes back empty (lock held by the new
        # owner). It must re-judge against the owner's committed fingerprint
        # (same payload) and wait -- a stale-fingerprint 409 would surface
        # here, long before the business ends.
        release_probe.set()
        await asyncio.sleep(0.3)
        assert not follower.done(), (
            "same-payload follower was refused instead of waiting for the owner"
        )
        release_business.set()
        reclaimer_resp = await asyncio.wait_for(reclaimer, 5)
        follower_resp = await asyncio.wait_for(follower, 5)

    assert reclaimer_resp.status_code == 201
    assert follower_resp.status_code == 201
    assert follower_resp.headers.get("x-idempotent-replay") == "true"
    assert follower_resp.json() == reclaimer_resp.json()
    assert calls["n"] == 1, "business must run exactly once"

    detail = (await client.get(f"/v1/traces/{trace}")).json()
    assert len(detail["spans"]) == 1

    # Ledger consistency: exactly one crash-leftover takeover (attempts 2);
    # the completed row carries the reclaimer's fingerprint and response.
    row = await _fetch_idempotency_row(scope, "reclaim-same-key")
    assert row.state == "COMPLETED"
    assert row.request_hash == hash_payload(
        scope, BatchIngestIn(**payload).model_dump(mode="json")
    )
    assert row.status_code == 201
    assert row.response_body["stored"] == 1
    assert row.response_body["results"] == [
        {"span_id": "root", "outcome": "stored"}
    ]
    assert row.attempts == 2

    # The key now replays the owner's payload and conflicts everything else.
    replay = await client.post(f"/v1/traces/{trace}/spans", json=payload, headers=key)
    assert replay.status_code == 201
    assert replay.headers.get("x-idempotent-replay") == "true"
    assert replay.json() == reclaimer_resp.json()
    conflict = await client.post(
        f"/v1/traces/{trace}/spans",
        json={"spans": [span_payload("other", duration_ms=999)]},
        headers=key,
    )
    assert conflict.status_code == 409


async def test_reclaim_race_different_payload_conflicts_without_waiting(
    client, monkeypatch
):
    """Same interleaving, but the parked follower carries a DIFFERENT payload
    than the reclaimer: once its probe loses to the new owner's business lock
    it must get 409 IDEMPOTENCY_CONFLICT immediately -- judged against the
    reclaimer's committed fingerprint, without waiting for the business
    transaction to end."""
    import asyncio

    from app.main import app
    from app.routers import ingest as ingest_router
    from app.schemas import BatchIngestIn
    from app.services.idempotency import hash_payload

    trace = "tr-reclaim-conflict"
    scope = f"ingest:{trace}"
    key = {"Idempotency-Key": "reclaim-conflict-key"}
    payload_owner = {"spans": [span_payload("root")]}
    payload_other = {"spans": [span_payload("other", duration_ms=999)]}

    await _seed_expired_pending(scope, "reclaim-conflict-key",
                                {"spans": [{"span_id": "crashed-owner"}]})

    (placeholder_seen, business_entered, release_probe,
     release_business, calls) = _hook_reclaim_interleaving(
        monkeypatch, scope, ingest_router, "ingest_spans"
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as c:
        follower = asyncio.create_task(
            c.post(f"/v1/traces/{trace}/spans", json=payload_other, headers=key)
        )
        await asyncio.wait_for(placeholder_seen.wait(), 5)
        reclaimer = asyncio.create_task(
            c.post(f"/v1/traces/{trace}/spans", json=payload_owner, headers=key)
        )
        await asyncio.wait_for(business_entered.wait(), 5)
        release_probe.set()
        # The 409 must come back while the owner's business is still gated.
        follower_resp = await asyncio.wait_for(follower, 5)
        assert not reclaimer.done(), "owner business finished before the 409"
        release_business.set()
        reclaimer_resp = await asyncio.wait_for(reclaimer, 5)

    assert follower_resp.status_code == 409
    assert follower_resp.json()["detail"]["error"] == "IDEMPOTENCY_CONFLICT"
    assert reclaimer_resp.status_code == 201
    assert calls["n"] == 1, "different-payload request must never run the business"

    detail = (await client.get(f"/v1/traces/{trace}")).json()
    assert [s["span_id"] for s in detail["spans"]] == ["root"]

    # Ledger consistency: the completed row kept the reclaimer's fingerprint
    # and response; exactly one takeover.
    row = await _fetch_idempotency_row(scope, "reclaim-conflict-key")
    assert row.state == "COMPLETED"
    assert row.request_hash == hash_payload(
        scope, BatchIngestIn(**payload_owner).model_dump(mode="json")
    )
    assert row.status_code == 201
    assert row.attempts == 2

    replay = await client.post(
        f"/v1/traces/{trace}/spans", json=payload_owner, headers=key
    )
    assert replay.status_code == 201
    assert replay.headers.get("x-idempotent-replay") == "true"
    assert replay.json() == reclaimer_resp.json()
    again = await client.post(
        f"/v1/traces/{trace}/spans", json=payload_other, headers=key
    )
    assert again.status_code == 409


async def test_reclaim_race_business_failure_reelects_waiter(client, monkeypatch):
    """Same interleaving, but the reclaimer's business FAILS while the
    same-payload follower waits on the lock: the row flips to FAILED, the
    follower wins the re-election and retries the business exactly once,
    completing the key; the ledger records the retried attempt's response."""
    import asyncio

    from fastapi import HTTPException

    from app.main import app
    from app.routers import ingest as ingest_router
    from app.schemas import BatchIngestIn
    from app.services.idempotency import hash_payload

    trace = "tr-reclaim-retry"
    scope = f"ingest:{trace}"
    key = {"Idempotency-Key": "reclaim-retry-key"}
    payload = {"spans": [span_payload("root")]}

    await _seed_expired_pending(scope, "reclaim-retry-key",
                                {"spans": [{"span_id": "crashed-owner"}]})

    boom = HTTPException(
        status_code=409,
        detail={"error": "SPAN_CONFLICT", "message": "injected business failure"},
    )
    (placeholder_seen, business_entered, release_probe,
     release_business, calls) = _hook_reclaim_interleaving(
        monkeypatch, scope, ingest_router, "ingest_spans",
        business_side_effect=boom,
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as c:
        follower = asyncio.create_task(
            c.post(f"/v1/traces/{trace}/spans", json=payload, headers=key)
        )
        await asyncio.wait_for(placeholder_seen.wait(), 5)
        reclaimer = asyncio.create_task(
            c.post(f"/v1/traces/{trace}/spans", json=payload, headers=key)
        )
        await asyncio.wait_for(business_entered.wait(), 5)
        release_probe.set()
        await asyncio.sleep(0.3)
        assert not follower.done(), (
            "same-payload follower was refused instead of waiting for the owner"
        )
        # The owner's business fails: SAVEPOINT rollback + FAILED published in
        # the same transaction; the waiting follower re-elects and retries.
        release_business.set()
        reclaimer_resp = await asyncio.wait_for(reclaimer, 5)
        follower_resp = await asyncio.wait_for(follower, 5)

    assert reclaimer_resp.status_code == 409
    assert reclaimer_resp.json()["detail"]["error"] == "SPAN_CONFLICT"
    assert follower_resp.status_code == 201
    assert follower_resp.json()["stored"] == 1
    assert calls["n"] == 2, "one failed attempt plus one controlled retry"

    detail = (await client.get(f"/v1/traces/{trace}")).json()
    assert len(detail["spans"]) == 1

    # Ledger consistency: the retried attempt completed the key with the
    # payload's fingerprint; the FAILED re-election is not a crash takeover
    # (attempts stays at the reclaimer's 2).
    row = await _fetch_idempotency_row(scope, "reclaim-retry-key")
    assert row.state == "COMPLETED"
    assert row.request_hash == hash_payload(
        scope, BatchIngestIn(**payload).model_dump(mode="json")
    )
    assert row.status_code == 201
    assert row.attempts == 2

    replay = await client.post(f"/v1/traces/{trace}/spans", json=payload, headers=key)
    assert replay.status_code == 201
    assert replay.headers.get("x-idempotent-replay") == "true"
    assert replay.json() == follower_resp.json()


async def test_reclaim_race_end_entry_waits_and_replays(client, monkeypatch):
    """The /end idempotent entry point shares the fixed judgement: under the
    same reclaim/business-lock interleaving the same-payload follower waits
    and replays the owner's published decision instead of being refused."""
    import asyncio

    from app.main import app
    from app.routers import ingest as ingest_router
    from app.services.idempotency import hash_payload

    trace = "tr-reclaim-end"
    scope = f"end:{trace}"
    key = {"Idempotency-Key": "reclaim-end-key"}
    await _post_spans(client, trace, [span_payload("root")])

    await _seed_expired_pending(scope, "reclaim-end-key", {"old": "payload"})

    (placeholder_seen, business_entered, release_probe,
     release_business, calls) = _hook_reclaim_interleaving(
        monkeypatch, scope, ingest_router, "finalize_trace"
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as c:
        follower = asyncio.create_task(
            c.post(f"/v1/traces/{trace}/end", headers=key)
        )
        await asyncio.wait_for(placeholder_seen.wait(), 5)
        reclaimer = asyncio.create_task(
            c.post(f"/v1/traces/{trace}/end", headers=key)
        )
        await asyncio.wait_for(business_entered.wait(), 5)
        release_probe.set()
        await asyncio.sleep(0.3)
        assert not follower.done(), (
            "same-payload follower was refused instead of waiting for the owner"
        )
        release_business.set()
        reclaimer_resp = await asyncio.wait_for(reclaimer, 5)
        follower_resp = await asyncio.wait_for(follower, 5)

    assert reclaimer_resp.status_code == 201
    assert follower_resp.status_code == 201
    assert follower_resp.headers.get("x-idempotent-replay") == "true"
    assert follower_resp.json() == reclaimer_resp.json()
    assert calls["n"] == 1, "finalization must run exactly once"

    decisions = (await client.get(f"/v1/traces/{trace}/revisions")).json()
    assert len(decisions) == 1

    row = await _fetch_idempotency_row(scope, "reclaim-end-key")
    assert row.state == "COMPLETED"
    assert row.request_hash == hash_payload(scope, {})
    assert row.status_code == 201
    assert row.attempts == 2


# ---------------------------------------------------------------------------
# Real-HTTP concurrency (subprocess uvicorn, real TCP, OS threads): timing,
# exactly-once business execution, and controlled retry after rollback.
# ---------------------------------------------------------------------------


def test_real_http_different_payload_conflicts_without_waiting(http_server):
    """The defect: a same-key/different-body request arriving while the first
    request's (slow) business transaction is open blocks at the unique index
    and only gets 409 after the leader finishes. After the fix the PENDING
    claim is already committed/visible, so 409 returns immediately."""
    import threading
    import time

    import httpx as _httpx

    trace = "tr-realhttp-conflict"
    key = {"Idempotency-Key": "realhttp-conflict-key"}
    payload_a = {"spans": [span_payload("a")]}
    payload_b = {"spans": [span_payload("b", duration_ms=999)]}

    _enable_slow_hooks(http_server, 1000)
    try:
        out = {}

        def leader():
            t0 = time.monotonic()
            r = _httpx.post(
                f"{http_server}/v1/traces/{trace}/spans",
                json=payload_a, headers=key, timeout=15,
            )
            out["a"] = (time.monotonic() - t0, r)

        def joiner():
            time.sleep(0.35)  # leader is mid business transaction now
            t0 = time.monotonic()
            r = _httpx.post(
                f"{http_server}/v1/traces/{trace}/spans",
                json=payload_b, headers=key, timeout=15,
            )
            out["b"] = (time.monotonic() - t0, r)

        t1 = threading.Thread(target=leader)
        t2 = threading.Thread(target=joiner)
        t1.start(); t2.start(); t2.join(); t1.join()
        calls = _hook_calls(http_server)["ingest_calls"]
    finally:
        _reset_slow_hooks(http_server)

    el_a, ra = out["a"]
    el_b, rb = out["b"]
    assert ra.status_code == 201
    assert rb.status_code == 409
    assert rb.json()["detail"]["error"] == "IDEMPOTENCY_CONFLICT"
    # The leader really took the slow path (~1s); the conflicting request did
    # not wait for it (well under the 1s business window).
    assert el_a >= 0.9, f"slow leader did not run: {el_a:.2f}s"
    assert el_b < 0.4, f"different-payload request waited for leader: {el_b:.2f}s"
    # Exactly one business execution -- the losing (different) body never ran.
    assert calls == 1

    detail = _httpx.get(f"{http_server}/v1/traces/{trace}").json()
    assert len(detail["spans"]) == 1
    assert detail["spans"][0]["span_id"] == "a"


def test_real_http_same_payload_waits_and_replays_once(http_server):
    """Same key + same body, genuinely simultaneous over HTTP: exactly one
    business execution; every follower waits for the leader and receives the
    identical 201 status and body with the replay header."""
    import httpx as _httpx

    trace = "tr-realhttp-once"
    key = {"Idempotency-Key": "realhttp-once-key"}
    payload = {"spans": [span_payload("root")]}

    _enable_slow_hooks(http_server, 1000)
    try:
        results = _post_concurrent(
            http_server,
            [(f"/v1/traces/{trace}/spans", payload, key) for _ in range(6)],
        )
        calls = _hook_calls(http_server)["ingest_calls"]
    finally:
        _reset_slow_hooks(http_server)

    assert all(not isinstance(r, Exception) for _, r in results)
    elapsed = [el for el, _ in results]
    responses = [r for _, r in results]
    assert [r.status_code for r in responses] == [201] * 6
    bodies = [r.json() for r in responses]
    assert all(b == bodies[0] for b in bodies)
    replay_flags = [r.headers.get("x-idempotent-replay") for r in responses]
    assert replay_flags.count("true") == 5
    # Followers wait for the slow leader; none returns before it finishes.
    assert min(elapsed) >= 0.9, f"request resolved before leader: {min(elapsed):.2f}s"
    assert calls == 1, f"business ran {calls} times, expected 1"

    detail = _httpx.get(f"{http_server}/v1/traces/{trace}").json()
    assert len(detail["spans"]) == 1

    # A later sequential same-key same-body call still replays byte-identically.
    again = _httpx.post(
        f"{http_server}/v1/traces/{trace}/spans", json=payload, headers=key
    )
    assert again.status_code == 201
    assert again.headers.get("x-idempotent-replay") == "true"
    assert again.json() == bodies[0]


def test_real_http_failure_rollback_allows_controlled_retry(http_server):
    """Leader business failure (409 SPAN_CONFLICT) is rolled back and waiters
    re-elect and retry; all bad-payload callers get SPAN_CONFLICT, the business
    runs once per attempt (never a unique-index 500), and the key is
    immediately reusable by a corrected request, which then replays."""
    import httpx as _httpx

    trace = "tr-realhttp-retry"
    seeded = _httpx.post(
        f"{http_server}/v1/traces/{trace}/spans",
        json={"spans": [span_payload("root")]}, timeout=15,
    )
    assert seeded.status_code == 201

    key = {"Idempotency-Key": "realhttp-retry-key"}
    bad_payload = {"spans": [span_payload("root", attributes={"x": 1})]}

    _enable_slow_hooks(http_server, 300)
    try:
        results = _post_concurrent(
            http_server,
            [(f"/v1/traces/{trace}/spans", bad_payload, key) for _ in range(4)],
        )
        calls = _hook_calls(http_server)["ingest_calls"]
    finally:
        _reset_slow_hooks(http_server)

    assert all(not isinstance(r, Exception) for _, r in results)
    responses = [r for _, r in results]
    assert [r.status_code for r in responses] == [409] * 4
    assert all(r.json()["detail"]["error"] == "SPAN_CONFLICT" for r in responses)
    # Leader + re-elected waiters: more than one attempt is allowed (each
    # failed transaction is rolled back and a new leader elected), but it is
    # bounded by the number of callers and never a 500/constraint error.
    assert 1 <= calls <= 4

    # Controlled retry with a corrected payload succeeds on the same key and
    # replays identical results afterwards.
    ok_payload = {"spans": [span_payload("child", "root")]}
    ok = _httpx.post(
        f"{http_server}/v1/traces/{trace}/spans", json=ok_payload, headers=key
    )
    assert ok.status_code == 201
    assert ok.json()["stored"] == 1

    replay = _httpx.post(
        f"{http_server}/v1/traces/{trace}/spans", json=ok_payload, headers=key
    )
    assert replay.status_code == 201
    assert replay.headers.get("x-idempotent-replay") == "true"
    assert replay.json() == ok.json()

    detail = _httpx.get(f"{http_server}/v1/traces/{trace}").json()
    assert {s["span_id"] for s in detail["spans"]} == {"root", "child"}


def test_real_http_end_same_payload_replays_single_decision(http_server):
    """The /end idempotent entry point over real HTTP: one finalization, all
    concurrent followers wait and replay the identical 201."""
    import httpx as _httpx

    trace = "tr-realhttp-end"
    seeded = _httpx.post(
        f"{http_server}/v1/traces/{trace}/spans",
        json={"spans": [span_payload("root")]}, timeout=15,
    )
    assert seeded.status_code == 201
    key = {"Idempotency-Key": "realhttp-end-key"}

    results = _post_concurrent(
        http_server,
        [(f"/v1/traces/{trace}/end", None, key) for _ in range(5)],
    )
    assert all(not isinstance(r, Exception) for _, r in results)
    responses = [r for _, r in results]
    assert [r.status_code for r in responses] == [201] * 5
    bodies = [r.json() for r in responses]
    assert all(b == bodies[0] for b in bodies)
    revisions = _httpx.get(f"{http_server}/v1/traces/{trace}/revisions").json()
    assert len(revisions) == 1


def test_real_http_policy_publish_different_payload_conflicts_fast(http_server):
    """The policies write entry point: a different-payload concurrent publish
    sharing an in-flight key gets 409 immediately instead of waiting for the
    CAS transaction; only the leader's body is published and replays."""
    import threading
    import time

    import httpx as _httpx

    active = _httpx.get(f"{http_server}/v1/policies/active").json()
    key = {"Idempotency-Key": "realhttp-publish-key"}
    body_a = {
        "name": "realhttp-policy-a",
        "expected_version": active["version"],
        "spec": {"default_decision": "DISCARD", "rules": []},
    }
    body_b = {**body_a, "name": "realhttp-policy-b"}

    _enable_slow_hooks(http_server, 1000)
    try:
        out = {}

        def leader():
            t0 = time.monotonic()
            r = _httpx.post(f"{http_server}/v1/policies", json=body_a,
                            headers=key, timeout=15)
            out["a"] = (time.monotonic() - t0, r)

        def joiner():
            time.sleep(0.35)
            t0 = time.monotonic()
            r = _httpx.post(f"{http_server}/v1/policies", json=body_b,
                            headers=key, timeout=15)
            out["b"] = (time.monotonic() - t0, r)

        t1 = threading.Thread(target=leader)
        t2 = threading.Thread(target=joiner)
        t1.start(); t2.start(); t2.join(); t1.join()
    finally:
        _reset_slow_hooks(http_server)

    el_a, ra = out["a"]
    el_b, rb = out["b"]
    assert ra.status_code == 201
    assert rb.status_code == 409
    assert rb.json()["detail"]["error"] == "IDEMPOTENCY_CONFLICT"
    assert el_a >= 0.9
    assert el_b < 0.4, f"different-payload publish waited: {el_b:.2f}s"
    # Only policy A was ever published; same-key replay of A returns it.
    replay = _httpx.post(f"{http_server}/v1/policies", json=body_a, headers=key)
    assert replay.status_code == 201
    assert replay.headers.get("x-idempotent-replay") == "true"
    assert replay.json()["name"] == "realhttp-policy-a"
