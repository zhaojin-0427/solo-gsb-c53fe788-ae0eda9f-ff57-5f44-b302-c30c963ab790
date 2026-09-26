"""Trace ingestion, finalization, watermark sweep and revision replay."""
from __future__ import annotations

from datetime import timedelta
from typing import Any

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.models import (
    CloseReason,
    Decision,
    Span,
    Trace,
    TraceState,
    utcnow,
)
from app.sampling.engine import build_trace_graph, evaluate_policy
from app.schemas import SpanIn
from app.services.policies import get_policy

settings = get_settings()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _val(value: Any) -> str:
    return value.value if hasattr(value, "value") else value


def _to_engine_span(row: Span) -> dict[str, Any]:
    return {
        "span_id": row.span_id,
        "parent_id": row.parent_id,
        "name": row.name,
        "start_ms": int(row.start_time.timestamp() * 1000),
        "end_ms": int(row.end_time.timestamp() * 1000),
        "duration_ms": row.duration_ms,
        "status": row.status,
        "attributes": row.attributes or {},
        "links": row.links or [],
    }


def _normalize_links(span: SpanIn) -> list[dict[str, Any]]:
    return [
        {
            "trace_id": link.trace_id,
            "span_id": link.span_id,
            "attributes": link.attributes,
            "is_error": link.is_error,
        }
        for link in span.links
    ]


def _conflict(span_id: str, reason: str) -> HTTPException:
    return HTTPException(
        status_code=409,
        detail={
            "error": "SPAN_CONFLICT",
            "span_id": span_id,
            "message": reason,
        },
    )


# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------


async def ingest_spans(
    session: AsyncSession, trace_id: str, spans: list[SpanIn]
) -> dict[str, Any]:
    """Insert out-of-order spans under a row-locked trace.

    Reopening a published trace starts a NEW revision; the previous decision
    is never rewritten.
    """
    now = utcnow()
    # Reserve the trace row first so two concurrent ingests for a brand-new
    # trace cannot both INSERT (duplicate PK would otherwise 500).
    await session.execute(
        pg_insert(Trace)
        .values(
            trace_id=trace_id,
            state=TraceState.OPEN.value,
            revision=1,
            first_seen_at=now,
            last_span_at=now,
            revision_started_at=now,
        )
        .on_conflict_do_nothing(index_elements=["trace_id"])
    )
    trace = await _lock_trace(session, trace_id)
    if trace is None:  # pragma: no cover - insert above guarantees the row
        raise HTTPException(status_code=500, detail="trace reservation failed")

    reopened = False
    if trace.state == TraceState.ARCHIVED.value:
        trace.revision += 1
        trace.state = TraceState.OPEN.value
        trace.policy_version = None
        trace.current_decision_id = None
        trace.last_span_at = now
        trace.revision_started_at = now
        revision = trace.revision
        reopened = True
    else:
        revision = trace.revision

    existing = {
        row.span_id: row
        for row in (
            await session.execute(select(Span).where(Span.trace_id == trace_id))
        ).scalars()
    }

    results: list[dict[str, str]] = []
    stored = 0
    duplicates = 0

    for incoming in spans:
        row = existing.get(incoming.span_id)
        if row is not None:
            if not _same_payload(row, incoming):
                raise _conflict(
                    incoming.span_id,
                    "span already exists with different content",
                )
            results.append({"span_id": incoming.span_id, "outcome": "duplicate"})
            duplicates += 1
            continue

        session.add(
            Span(
                trace_id=trace_id,
                span_id=incoming.span_id,
                parent_id=incoming.parent_id,
                name=incoming.name,
                start_time=incoming.start_time,
                end_time=incoming.end_time,
                duration_ms=incoming.duration_ms,
                status=incoming.status,
                attributes=incoming.attributes,
                links=_normalize_links(incoming),
                revision=revision,
                received_at=now,
            )
        )
        results.append({"span_id": incoming.span_id, "outcome": "stored"})
        stored += 1

    if stored:
        trace.last_span_at = now
    await session.flush()

    return {
        "trace_id": trace_id,
        "revision": revision,
        "state": _val(trace.state),
        "received": len(spans),
        "stored": stored,
        "duplicates": duplicates,
        "results": results,
        "reopened": reopened,
    }


def _same_payload(row: Span, incoming: SpanIn) -> bool:
    return (
        row.parent_id == incoming.parent_id
        and row.name == incoming.name
        and row.start_time == incoming.start_time
        and row.end_time == incoming.end_time
        and row.duration_ms == incoming.duration_ms
        and row.status == incoming.status
        and (row.attributes or {}) == (incoming.attributes or {})
        and (row.links or []) == _normalize_links(incoming)
    )


async def _lock_trace(session: AsyncSession, trace_id: str) -> Trace | None:
    return (
        await session.execute(
            select(Trace).where(Trace.trace_id == trace_id).with_for_update()
        )
    ).scalar_one_or_none()


# ---------------------------------------------------------------------------
# Finalization
# ---------------------------------------------------------------------------


async def finalize_trace(
    session: AsyncSession,
    trace_id: str,
    close_reason: CloseReason,
    *,
    policy_version: int | None = None,
) -> tuple[Trace, Decision | None, bool]:
    """Archive one trace. Idempotent: an already archived trace returns its
    current decision. The trace row lock + unique (trace_id, revision)
    constraint make concurrent finalizers safe.

    Returns (trace, decision, created); ``created`` is True only for the
    caller that actually published the decision.
    """
    trace = await _lock_trace(session, trace_id)
    if trace is None:
        raise HTTPException(status_code=404, detail=f"trace {trace_id} not found")
    if trace.state == TraceState.ARCHIVED.value:
        decision = await session.get(Decision, trace.current_decision_id)
        return trace, decision, False

    now = utcnow()

    if close_reason == CloseReason.WATERMARK:
        # Un-closed trace crossed the hard watermark: mark INDETERMINATE
        # regardless of what the policy rules would say.
        policy = (
            await get_policy(session, policy_version)
            if policy_version is not None
            else await _get_active(session)
        )
        span_rows = list(
            (
                await session.execute(
                    select(Span).where(
                        Span.trace_id == trace_id, Span.revision == trace.revision
                    )
                )
            ).scalars()
        )
        graph = build_trace_graph([_to_engine_span(r) for r in span_rows])
        policy_eval = evaluate_policy(graph, policy.spec)
        evidence = {
            "close_reason": close_reason.value,
            "watermark_ms": settings.hard_watermark_ms,
            "revision_started_at": trace.revision_started_at.isoformat(),
            "closed_at": now.isoformat(),
            "note": (
                "trace was not explicitly closed and crossed the hard "
                "watermark; the rule evaluation below is advisory only"
            ),
            "would_have_verdict": policy_eval["verdict"],
            **policy_eval,
        }
        decision = Decision(
            trace_id=trace_id,
            revision=trace.revision,
            policy_version=policy.version,
            verdict="INDETERMINATE",
            close_reason=close_reason,
            span_count=graph["span_count"],
            evidence=evidence,
        )
    else:
        policy = (
            await get_policy(session, policy_version)
            if policy_version is not None
            else await _get_active(session)
        )
        span_rows = list(
            (
                await session.execute(
                    select(Span).where(
                        Span.trace_id == trace_id, Span.revision == trace.revision
                    )
                )
            ).scalars()
        )
        graph = build_trace_graph([_to_engine_span(r) for r in span_rows])
        policy_eval = evaluate_policy(graph, policy.spec)
        evidence = {
            "close_reason": close_reason.value,
            "window_ms": settings.window_ms,
            "closed_at": now.isoformat(),
            **policy_eval,
        }
        decision = Decision(
            trace_id=trace_id,
            revision=trace.revision,
            policy_version=policy.version,
            verdict=policy_eval["verdict"],
            close_reason=close_reason,
            span_count=graph["span_count"],
            evidence=evidence,
        )

    session.add(decision)
    await session.flush()  # flush to get decision.id before wiring the trace

    trace.state = TraceState.ARCHIVED.value
    trace.policy_version = decision.policy_version
    trace.current_decision_id = decision.id
    return trace, decision, True


async def _get_active(session: AsyncSession):
    from app.services.policies import get_active_policy

    return await get_active_policy(session)


# ---------------------------------------------------------------------------
# Background sweep
# ---------------------------------------------------------------------------


async def sweep_once(session: AsyncSession) -> dict[str, int]:
    """Close due traces. Watermarks are processed before idle windows so an
    over-watermark trace is always recorded as INDETERMINATE.
    """
    now = utcnow()
    watermark_deadline = now - timedelta(seconds=settings.hard_watermark_seconds)
    window_deadline = now - timedelta(seconds=settings.window_seconds)

    over_watermark = list(
        (
            await session.execute(
                select(Trace.trace_id)
                .where(Trace.state == TraceState.OPEN.value)
                .where(Trace.revision_started_at <= watermark_deadline)
                .order_by(Trace.revision_started_at)
            )
        ).scalars()
    )
    watermarked = 0
    for trace_id in over_watermark:
        try:
            await finalize_trace(
                session, trace_id, CloseReason.WATERMARK
            )
            await session.commit()
            watermarked += 1
        except Exception:
            await session.rollback()

    idle = list(
        (
            await session.execute(
                select(Trace.trace_id)
                .where(Trace.state == TraceState.OPEN.value)
                .where(Trace.last_span_at <= window_deadline)
                .order_by(Trace.last_span_at)
            )
        ).scalars()
    )
    window_closed = 0
    for trace_id in idle:
        try:
            await finalize_trace(session, trace_id, CloseReason.WINDOW)
            await session.commit()
            window_closed += 1
        except Exception:
            await session.rollback()

    return {"watermarked": watermarked, "window_closed": window_closed}


# ---------------------------------------------------------------------------
# Reads & replay
# ---------------------------------------------------------------------------


async def get_trace(session: AsyncSession, trace_id: str) -> Trace:
    trace = await session.get(Trace, trace_id)
    if trace is None:
        raise HTTPException(status_code=404, detail=f"trace {trace_id} not found")
    return trace


async def list_trace_spans(session: AsyncSession, trace_id: str) -> list[Span]:
    return list(
        (
            await session.execute(
                select(Span)
                .where(Span.trace_id == trace_id)
                .order_by(Span.start_time, Span.span_id)
            )
        ).scalars()
    )


async def get_decision(
    session: AsyncSession, trace_id: str, revision: int | None = None
) -> Decision:
    trace = await get_trace(session, trace_id)
    stmt = select(Decision).where(Decision.trace_id == trace_id)
    if revision is not None:
        stmt = stmt.where(Decision.revision == revision)
    else:
        stmt = stmt.where(Decision.id == trace.current_decision_id)
    decision = (await session.execute(stmt)).scalar_one_or_none()
    if decision is None:
        raise HTTPException(
            status_code=404,
            detail=f"no decision for trace {trace_id}"
            + (f" revision {revision}" if revision is not None else ""),
        )
    return decision


async def list_revisions(session: AsyncSession, trace_id: str) -> list[Decision]:
    await get_trace(session, trace_id)
    return list(
        (
            await session.execute(
                select(Decision)
                .where(Decision.trace_id == trace_id)
                .order_by(Decision.revision)
            )
        ).scalars()
    )


async def replay_revision(
    session: AsyncSession,
    trace_id: str,
    revision: int,
    requested_policy_version: int,
) -> dict[str, Any]:
    """Re-run the engine against the exact span snapshot of a revision.

    Purely read-only: never mutates the published decision.
    """
    decision = await get_decision(session, trace_id, revision)
    policy = await get_policy(session, requested_policy_version)

    span_rows = list(
        (
            await session.execute(
                select(Span)
                .where(Span.trace_id == trace_id, Span.revision == revision)
                .order_by(Span.start_time, Span.span_id)
            )
        ).scalars()
    )
    graph = build_trace_graph([_to_engine_span(r) for r in span_rows])
    evidence = evaluate_policy(graph, policy.spec)
    stored = _val(decision.verdict)
    return {
        "trace_id": trace_id,
        "revision": revision,
        "requested_policy_version": requested_policy_version,
        "policy_version": policy.version,
        "verdict": evidence["verdict"],
        "close_reason": _val(decision.close_reason),
        "span_count": graph["span_count"],
        "evidence": evidence,
        "stored_verdict": stored,
        "matches_published": stored == evidence["verdict"],
    }
