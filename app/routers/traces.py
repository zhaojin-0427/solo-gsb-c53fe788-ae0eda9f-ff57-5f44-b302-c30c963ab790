"""Trace query, decision explanation and revision replay."""
from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_session
from app.schemas import (
    DecisionSummary,
    LinkOut,
    ReplayResponse,
    SpanOut,
    TraceDetailOut,
)
from app.services.traces import (
    get_decision,
    get_trace,
    list_revisions,
    list_trace_spans,
    replay_revision,
)
from app.services.policies import get_active_policy

router = APIRouter(prefix="/v1/traces", tags=["queries"])


def _span_out(row) -> SpanOut:
    return SpanOut(
        span_id=row.span_id,
        parent_id=row.parent_id,
        name=row.name,
        start_time=row.start_time,
        end_time=row.end_time,
        duration_ms=row.duration_ms,
        status=row.status if isinstance(row.status, str) else row.status.value,
        attributes=row.attributes or {},
        links=[LinkOut(**link) for link in (row.links or [])],
        revision=row.revision,
    )


@router.get("/{trace_id}", response_model=TraceDetailOut)
async def read_trace(trace_id: str, session: AsyncSession = Depends(get_session)):
    trace = await get_trace(session, trace_id)
    spans = await list_trace_spans(session, trace_id)

    def v(x):
        return x.value if hasattr(x, "value") else x

    return TraceDetailOut(
        trace_id=trace.trace_id,
        state=v(trace.state),
        revision=trace.revision,
        policy_version=trace.policy_version,
        current_decision_id=trace.current_decision_id,
        first_seen_at=trace.first_seen_at,
        last_span_at=trace.last_span_at,
        revision_started_at=trace.revision_started_at,
        spans=[_span_out(r) for r in spans],
    )


def _decision_out(row) -> DecisionSummary:
    def v(x):
        return x.value if hasattr(x, "value") else x

    return DecisionSummary(
        id=row.id,
        revision=row.revision,
        policy_version=row.policy_version,
        verdict=v(row.verdict),
        close_reason=v(row.close_reason),
        span_count=row.span_count,
        evidence=row.evidence,
        created_at=row.created_at,
    )


@router.get("/{trace_id}/decision", response_model=DecisionSummary)
async def read_decision(
    trace_id: str,
    revision: int | None = Query(default=None),
    session: AsyncSession = Depends(get_session),
):
    """Decision + evidence (the explanation)."""
    decision = await get_decision(session, trace_id, revision)
    return _decision_out(decision)


@router.get("/{trace_id}/revisions", response_model=list[DecisionSummary])
async def read_revisions(trace_id: str, session: AsyncSession = Depends(get_session)):
    return [_decision_out(d) for d in await list_revisions(session, trace_id)]


@router.post("/{trace_id}/revisions/{revision}/replay", response_model=ReplayResponse)
async def replay(
    trace_id: str,
    revision: int,
    policy_version: int | None = Query(
        default=None,
        description="policy to replay with; defaults to the current active policy",
    ),
    session: AsyncSession = Depends(get_session),
):
    """Re-evaluate a frozen revision with any policy version. Read-only."""
    if policy_version is None:
        active = await get_active_policy(session)
        policy_version = active.version
    return await replay_revision(session, trace_id, revision, policy_version)
