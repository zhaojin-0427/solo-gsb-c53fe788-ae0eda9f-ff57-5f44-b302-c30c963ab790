"""Policy publishing (CAS) and listing."""
from __future__ import annotations

from fastapi import APIRouter, Depends, Header, Response
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_session
from app.schemas import PolicyOut, PublishPolicyIn
from app.services import idempotency
from app.services.policies import (
    get_active_policy,
    get_policy,
    list_policies,
    publish_policy,
)

router = APIRouter(prefix="/v1/policies", tags=["policies"])


def _policy_out(row) -> PolicyOut:
    return PolicyOut(
        version=row.version,
        name=row.name,
        spec=row.spec,
        is_active=row.is_active,
        created_at=row.created_at,
        created_by=row.created_by,
    )


@router.get("", response_model=list[PolicyOut])
async def read_policies(session: AsyncSession = Depends(get_session)):
    return [_policy_out(p) for p in await list_policies(session)]


@router.get("/active", response_model=PolicyOut)
async def read_active_policy(session: AsyncSession = Depends(get_session)):
    return _policy_out(await get_active_policy(session))


@router.get("/{version}", response_model=PolicyOut)
async def read_policy(version: int, session: AsyncSession = Depends(get_session)):
    return _policy_out(await get_policy(session, version))


@router.post("", response_model=PolicyOut, status_code=201)
async def publish(
    body: PublishPolicyIn,
    response: Response,
    idempotency_key: str | None = Header(default=None),
    session: AsyncSession = Depends(get_session),
):
    """Publish a new policy version with compare-and-set semantics.

    `expected_version` must equal the current active version (omit it only
    when no policy exists yet). Concurrent mismatched publishes get 409.
    Supports Idempotency-Key: same key + same payload replays the published
    version instead of attempting (and failing) a second CAS.
    """
    payload = body.model_dump(mode="json")

    async def run() -> tuple[int, dict]:
        policy, previous = await publish_policy(session, body)
        out = _policy_out(policy).model_dump(mode="json")
        return 201, {"policy": out, "previous": previous}

    if idempotency_key is None:
        status_code, result = await run()
        replayed = False
    else:
        result, status_code, replayed = await idempotency.run_idempotent(
            session, "policies:publish", idempotency_key, payload, run
        )

    response.status_code = status_code
    if replayed:
        response.headers[idempotency.REPLAY_HEADER] = "true"
    previous = result["previous"]
    response.headers["X-Previous-Version"] = str(previous) if previous else ""
    return result["policy"]
