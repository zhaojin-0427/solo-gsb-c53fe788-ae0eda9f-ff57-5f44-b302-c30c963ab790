"""Span ingestion + explicit trace end."""
from __future__ import annotations

from fastapi import APIRouter, Depends, Header, Response
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_session
from app.models import CloseReason
from app.schemas import BatchIngestIn, FinalizeResponse, IngestResponse
from app.services import idempotency
from app.services.traces import finalize_trace, ingest_spans

router = APIRouter(prefix="/v1/traces", tags=["traces"])


def _serialize_decision(decision) -> dict:
    def v(x):
        return x.value if hasattr(x, "value") else x

    return {
        "id": decision.id,
        "revision": decision.revision,
        "policy_version": decision.policy_version,
        "verdict": v(decision.verdict),
        "close_reason": v(decision.close_reason),
        "span_count": decision.span_count,
        "evidence": decision.evidence,
        "created_at": decision.created_at,
    }


def _serialize_trace(trace) -> dict:
    def v(x):
        return x.value if hasattr(x, "value") else x

    return {
        "trace_id": trace.trace_id,
        "state": v(trace.state),
        "revision": trace.revision,
        "policy_version": trace.policy_version,
        "current_decision_id": trace.current_decision_id,
        "first_seen_at": trace.first_seen_at,
        "last_span_at": trace.last_span_at,
        "revision_started_at": trace.revision_started_at,
    }


@router.post("/{trace_id}/spans", response_model=IngestResponse)
async def report_spans(
    trace_id: str,
    body: BatchIngestIn,
    response: Response,
    idempotency_key: str | None = Header(default=None),
    session: AsyncSession = Depends(get_session),
):
    """Report a batch of possibly out-of-order spans.

    Supports the Idempotency-Key header. Concurrent first calls with the
    same key and the same payload are serialized by an atomic PENDING
    placeholder: the business write runs exactly once and every other
    request blocks until it can replay the identical status code and body.
    A reused key with a different payload yields 409.
    """
    payload = body.model_dump(mode="json")

    async def run() -> tuple[int, dict]:
        result = await ingest_spans(session, trace_id, body.spans)
        if result.pop("reopened"):
            result["note"] = "trace reopened as new revision"
        status_code = 201 if result["stored"] else 200
        return status_code, result

    if idempotency_key is None:
        status_code, result = await run()
        replayed = False
    else:
        result, status_code, replayed = await idempotency.run_idempotent(
            session, f"ingest:{trace_id}", idempotency_key, payload, run
        )

    response.status_code = status_code
    if replayed:
        response.headers[idempotency.REPLAY_HEADER] = "true"
    return result


@router.post("/{trace_id}/end", response_model=FinalizeResponse)
async def end_trace(
    trace_id: str,
    response: Response,
    idempotency_key: str | None = Header(default=None),
    session: AsyncSession = Depends(get_session),
):
    """Explicitly close a trace and decide immediately.

    Concurrent/duplicate calls never archive twice: the first closes, later
    calls return the already-published decision. With an Idempotency-Key,
    concurrent first calls block on the atomic placeholder and replay the
    first response (same status and body) instead of racing.
    """

    async def run() -> tuple[int, dict]:
        trace, decision, created = await finalize_trace(
            session, trace_id, CloseReason.END_SIGNAL
        )
        note = (
            None
            if created
            else "trace was already archived; returning the published decision"
        )
        body = {
            "trace": _serialize_trace(trace),
            "decision": _serialize_decision(decision) if decision else None,
            "note": note,
        }
        return 200 if note else 201, body

    if idempotency_key is None:
        status_code, body = await run()
        replayed = False
    else:
        body, status_code, replayed = await idempotency.run_idempotent(
            session, f"end:{trace_id}", idempotency_key, {}, run
        )

    response.status_code = status_code
    if replayed:
        response.headers[idempotency.REPLAY_HEADER] = "true"
    return body
