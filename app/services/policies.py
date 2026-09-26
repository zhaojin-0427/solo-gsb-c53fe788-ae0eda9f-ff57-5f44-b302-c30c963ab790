"""Policy publishing with compare-and-set (CAS)."""
from __future__ import annotations

from fastapi import HTTPException
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Policy
from app.schemas import PublishPolicyIn


async def get_active_policy(session: AsyncSession) -> Policy:
    policy = (
        await session.execute(select(Policy).where(Policy.is_active.is_(True)))
    ).scalar_one_or_none()
    if policy is None:
        raise HTTPException(status_code=503, detail="No active policy published")
    return policy


async def get_policy(session: AsyncSession, version: int) -> Policy:
    policy = await session.get(Policy, version)
    if policy is None:
        raise HTTPException(status_code=404, detail=f"policy version {version} not found")
    return policy


async def list_policies(session: AsyncSession) -> list[Policy]:
    return list(
        (
            await session.execute(select(Policy).order_by(Policy.version.desc()))
        ).scalars()
    )


async def publish_policy(
    session: AsyncSession, body: PublishPolicyIn
) -> tuple[Policy, int | None]:
    """CAS publish.

    pg_advisory_xact_lock serializes publishers so two concurrent publishes
    with the same expected_version cannot both succeed.
    Returns (new_policy, previous_version).
    """
    await session.execute(text("SELECT pg_advisory_xact_lock(91720)"))

    current = (
        await session.execute(select(Policy).where(Policy.is_active.is_(True)))
    ).scalar_one_or_none()
    current_version = current.version if current else None

    if body.expected_version != current_version:
        raise HTTPException(
            status_code=409,
            detail={
                "error": "CAS_CONFLICT",
                "message": "expected_version does not match current active version",
                "expected_version": body.expected_version,
                "current_version": current_version,
            },
        )

    if current is not None:
        current.is_active = False

    spec_dict = body.spec.model_dump(mode="json")
    policy = Policy(
        name=body.name,
        spec=spec_dict,
        is_active=True,
        created_by=body.created_by,
    )
    session.add(policy)
    await session.flush()
    return policy, current_version
