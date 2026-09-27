"""Idempotency-Key handling shared by every state-changing POST endpoint.

Concurrency protocol -- **two transactions** (this is what lets a same-key,
different-payload request fail *immediately* instead of blocking on the
leader's business transaction):

Phase 1 -- the *claim* (a short transaction on its own connection, committed
before any business work):

1. The first writer atomically ``INSERT ... ON CONFLICT (scope, key) DO
   NOTHING`` a PENDING placeholder carrying the SHA-256 request fingerprint
   and a freshly generated ``run_token``. Because this claim commits
   immediately, the placeholder (and its fingerprint) is visible to peers
   under READ COMMITTED while the leader is still working.
2. A concurrent peer whose INSERT finds the row classifies it **without
   disturbing a live leader**:
     * COMPLETED + same fingerprint -> replay (identical status/body);
     * COMPLETED/locked-PENDING + different fingerprint -> immediate 409;
     * FAILED -> one peer wins an UPDATE election back to PENDING (new
       run_token, any fingerprint) and becomes the retry leader; the others
       loop and attach;
     * PENDING whose FOR UPDATE lock is held -> a live leader: the
       same/different-payload decision is re-judged against the CURRENT
       committed owner's fingerprint (the row is re-read with
       ``populate_existing``, because an expired placeholder may have been
       reclaimed -- new fingerprint committed -- by the very peer now
       holding the lock after our unlocked read): same fingerprint waits
       in phase 2, different fingerprint gets 409 now;
     * PENDING whose lock is free -> either the millisecond-wide hand-off
       gap right after a claim commits, or a crash leftover older than
       ``TS_IDEMPOTENCY_PENDING_TTL_MS`` (reclaimed, attempts bumped).

   Every classification read uses ``populate_existing`` so the ORM identity
   map can never serve a snapshot cached by an earlier statement of the same
   claim session. The takeover probe in particular re-reads the row **under
   the FOR UPDATE lock it just acquired** and re-classifies from that
   authoritative state: if the leader committed COMPLETED while the peer was
   classifying (possible once the business outlives the TTL), the completed
   row keeps the first writer's fingerprint, status code and body -- the
   peer replays it (same payload) or gets 409 (different payload), and only
   a row that is *still* PENDING, unlocked and expired under the lock is
   reclaimed. Without this, the probe would decide on the stale identity-map
   object and its "reclaim" UPDATE would rewrite the fingerprint of the
   completed row while keeping its response -- replaying the first writer's
   result for a different-payload request instead of returning 409.

Phase 2 -- the *business* (the request's own session/transaction):

3. The leader takes ``SELECT ... FOR UPDATE`` on the claim row and holds that
   lock until its business transaction ends, then verifies its run_token
   still owns the row (guards the tiny claim-commit -> lock-acquire gap).
4. Same-payload followers take the same FOR UPDATE lock, which blocks while
   the leader's business transaction is open; when they wake the row is
   either COMPLETED (replay) or FAILED (loop to the phase-1 re-election).
   Different-payload callers never get here: they failed in phase 1.
5. If the leader's business work raises, a SAVEPOINT rollback undoes every
   business write while the claim-row lock is retained; the row is flipped
   to FAILED in the SAME transaction and committed -- so waiting followers
   unblock directly onto FAILED (no invisible gap, no busy waiting) and
   exactly one of them wins re-election and retries. The business logic
   therefore runs at most once per *commit*, and a unique-constraint 500 is
   impossible. If every waiter also fails, the key is left FAILED and a
   fresh (possibly corrected) request claims it immediately.
6. A hard-crashed leader leaves a PENDING row that nobody holds a lock on;
   once it is older than the TTL a same-payload (or, since no response was
   ever published, any-payload) request reclaims it.

Same key + same payload always yields the exact same status code and body;
same key + different payload always yields 409 without running the business
or waiting for an in-flight leader.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Awaitable, Callable

from fastapi import HTTPException
from fastapi.encoders import jsonable_encoder
from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.db import SessionLocal
from app.models import IdempotencyKey, IdempotencyState, utcnow

# Header added on responses served from the stored ledger (the status code
# and body are still byte-for-byte identical to the first response).
REPLAY_HEADER = "X-Idempotent-Replay"

_MAX_KEY_LEN = 256

# Backoff for the only non-lock-serialized window (claim committed, business
# transaction has not taken the FOR UPDATE lock yet). A real leader queues on
# the lock within microseconds, so one short pause is always enough.
_GAP_POLL_SECONDS = 0.02

settings = get_settings()


def hash_payload(scope: str, payload: Any) -> str:
    body = json.dumps(
        {"scope": scope, "payload": payload}, sort_keys=True, default=str
    ).encode()
    return hashlib.sha256(body).hexdigest()


def validate_key(key: str) -> None:
    """Reject keys the ledger column cannot store (would otherwise 500)."""
    if len(key) > _MAX_KEY_LEN:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "IDEMPOTENCY_KEY_TOO_LONG",
                "message": f"Idempotency-Key must be at most {_MAX_KEY_LEN} characters",
            },
        )


def _conflict(key: str) -> HTTPException:
    return HTTPException(
        status_code=409,
        detail={
            "error": "IDEMPOTENCY_CONFLICT",
            "message": "Idempotency-Key was already used with a different payload",
            "key": key,
        },
    )


def _new_token() -> str:
    return uuid.uuid4().hex


@dataclass
class _Claim:
    """Result of phase 1: run the business as ``leader`` (owner of
    ``run_token``), attach in phase 2 as a waiter, or replay a COMPLETED row
    (``leader`` False with ``run_token`` None)."""

    leader: bool
    run_token: str | None = None
    status_code: int | None = None
    body: Any = None


async def _read(session: AsyncSession, scope: str, key: str):
    """Unlocked re-read of the claim row.

    ``populate_existing`` forces the row to be reloaded from the database
    even when this claim session's identity map already holds it: every
    classification (and every re-election loop iteration) must see the
    authoritative committed state, never a snapshot cached by an earlier
    statement of the same session -- otherwise a peer that once read FAILED
    would re-elect against a long-gone state forever.
    """
    return (
        await session.execute(
            select(IdempotencyKey)
            .where(
                IdempotencyKey.scope == scope, IdempotencyKey.key == key
            )
            .execution_options(populate_existing=True)
        )
    ).scalar_one_or_none()


async def _claim_in_own_transaction(
    scope: str, key: str, request_hash: str
) -> _Claim:
    """Phase 1. Runs on a *separate* connection so the PENDING placeholder is
    committed (hence visible to peers) before the business transaction opens.
    """
    now = utcnow()
    stale_before = now - timedelta(
        seconds=settings.idempotency_pending_ttl_seconds
    )
    async with SessionLocal() as cs:
        while True:
            token = _new_token()
            row = (
                await cs.execute(
                    pg_insert(IdempotencyKey)
                    .values(
                        scope=scope,
                        key=key,
                        request_hash=request_hash,
                        state=IdempotencyState.PENDING.value,
                        status_code=None,
                        response_body=None,
                        run_token=token,
                        attempts=1,
                        locked_at=now,
                        completed_at=None,
                        created_at=now,
                    )
                    .on_conflict_do_nothing(index_elements=["scope", "key"])
                    .returning(IdempotencyKey)
                )
            ).scalar_one_or_none()
            if row is not None:
                # Won the first-time insert: leader, lock taken in phase 2.
                await cs.commit()
                return _Claim(leader=True, run_token=token)

            # The claim already exists. Re-read WITHOUT a long lock so a live
            # leader's business transaction is never blocked.
            row = await _read(cs, scope, key)
            same_fp = row.request_hash == request_hash

            if row.state == IdempotencyState.COMPLETED.value:
                if not same_fp:
                    raise _conflict(key)
                return _Claim(
                    leader=False,
                    run_token=None,
                    status_code=row.status_code,
                    body=row.response_body,
                )

            if row.state == IdempotencyState.FAILED.value:
                # Nobody owns the key and no response was ever published: a
                # corrected payload may take over too. Atomic re-election --
                # exactly one waiter flips FAILED -> PENDING.
                elected = (
                    await cs.execute(
                        update(IdempotencyKey)
                        .where(
                            IdempotencyKey.scope == scope,
                            IdempotencyKey.key == key,
                            IdempotencyKey.state == IdempotencyState.FAILED.value,
                        )
                        .values(
                            state=IdempotencyState.PENDING.value,
                            request_hash=request_hash,
                            run_token=token,
                            status_code=None,
                            response_body=None,
                            completed_at=None,
                            locked_at=utcnow(),
                        )
                        .returning(IdempotencyKey)
                    )
                ).scalar_one_or_none()
                await cs.commit()
                if elected is not None:
                    return _Claim(leader=True, run_token=token)
                # Lost the election (another waiter won, or a COMPLETED row
                # landed): loop and reclassify.
                continue

            # PENDING. Distinguish a live leader from a crash leftover /
            # hand-off gap without waiting: SKIP LOCKED grabs the row only
            # when nobody's business transaction holds it. populate_existing
            # makes the probe double as the authoritative re-read *under the
            # lock*: the plain read above may be a stale identity-map
            # snapshot (e.g. the leader committed COMPLETED while we were
            # classifying, which is possible once the business outlives the
            # TTL), and the takeover decision must be made on the locked
            # row's real state -- never on the cached object.
            unlocked = (
                await cs.execute(
                    select(IdempotencyKey)
                    .where(
                        IdempotencyKey.scope == scope, IdempotencyKey.key == key
                    )
                    .with_for_update(skip_locked=True)
                    .execution_options(populate_existing=True)
                )
            ).scalar_one_or_none()

            if unlocked is not None:
                # We hold the row lock; ``unlocked`` carries the locked row's
                # authoritative committed state. Re-classify from it.
                locked_hash = unlocked.request_hash

                if unlocked.state == IdempotencyState.COMPLETED.value:
                    # The leader finished while we were classifying. A
                    # COMPLETED row is immutable: keep the first writer's
                    # fingerprint, status code and response body -- replay it
                    # (same payload) or conflict (different payload); never
                    # reclaim it.
                    status_code, body = unlocked.status_code, unlocked.response_body
                    await cs.rollback()  # release the lock; row untouched
                    if locked_hash != request_hash:
                        raise _conflict(key)
                    return _Claim(
                        leader=False,
                        run_token=None,
                        status_code=status_code,
                        body=body,
                    )

                if unlocked.state == IdempotencyState.FAILED.value:
                    # The leader failed while we were classifying: release
                    # the lock and contest the atomic re-election above.
                    await cs.rollback()
                    continue

                # Still PENDING under the lock.
                if unlocked.locked_at > stale_before:
                    # Fresh and unlocked: the leader is in the millisecond
                    # window between claim commit and acquiring its business
                    # lock. Don't run anything ourselves; let phase 2 line us
                    # up behind (or briefly before) the true leader.
                    await cs.rollback()
                    if locked_hash != request_hash:
                        raise _conflict(key)
                    return _Claim(leader=False, run_token=token)

                # Crash leftover past its TTL, verified under the lock:
                # reclaim it (any fingerprint).
                unlocked.request_hash = request_hash
                unlocked.state = IdempotencyState.PENDING.value
                unlocked.run_token = token
                unlocked.status_code = None
                unlocked.response_body = None
                unlocked.completed_at = None
                unlocked.attempts += 1
                unlocked.locked_at = utcnow()
                await cs.commit()
                return _Claim(leader=True, run_token=token)

            # The lock is held by a live leader's open business transaction.
            # The row classified above may predate a takeover: an expired
            # PENDING placeholder can be reclaimed (new fingerprint
            # committed) by a peer after our unlocked read, and that peer is
            # then the lock holder our probe just lost to. The
            # same/different-payload decision must therefore be re-judged
            # against the CURRENT committed owner's fingerprint -- re-read
            # the row (populate_existing defeats the identity map) instead of
            # trusting the possibly stale snapshot, or a request carrying the
            # owner's exact payload would be wrongly refused 409.
            current = await _read(cs, scope, key)

            if current.state == IdempotencyState.COMPLETED.value:
                # The leader finished between the probe and this re-read.
                if current.request_hash != request_hash:
                    raise _conflict(key)
                return _Claim(
                    leader=False,
                    run_token=None,
                    status_code=current.status_code,
                    body=current.response_body,
                )

            if current.state == IdempotencyState.FAILED.value:
                # The leader failed in that window: contest the re-election.
                continue

            # Still PENDING under the live leader: ``current.request_hash``
            # is the committed owner's authoritative fingerprint.
            if current.request_hash != request_hash:
                raise _conflict(key)
            # Same payload as the current owner: phase 2 blocks on the lock
            # and replays / retries.
            return _Claim(leader=False, run_token=token)


async def _lock_claim(session: AsyncSession, scope: str, key: str):
    return (
        await session.execute(
            select(IdempotencyKey)
            .where(IdempotencyKey.scope == scope, IdempotencyKey.key == key)
            .with_for_update()
        )
    ).scalar_one()


async def run_idempotent(
    session: AsyncSession,
    scope: str,
    key: str,
    payload: Any,
    handler: Callable[[], Awaitable[tuple[int, Any]]],
) -> tuple[Any, int, bool]:
    """Run ``handler`` exactly once per committed (key, payload) pair.

    ``handler`` is awaited only for a leader and returns
    ``(status_code, body)``. Followers never execute it.

    Returns ``(body, status_code, replayed)``; ``replayed`` is True when the
    result came from the stored ledger rather than a fresh execution.
    """
    validate_key(key)
    request_hash = hash_payload(scope, payload)

    while True:
        claim = await _claim_in_own_transaction(scope, key, request_hash)
        if claim.run_token is None:
            # Settled COMPLETED row: replay immediately.
            return claim.body, claim.status_code, True

        # Phase 2: block on / take the claim row lock for the duration of the
        # business transaction.
        row = await _lock_claim(session, scope, key)

        if row.state == IdempotencyState.COMPLETED.value:
            if row.request_hash != request_hash:
                await session.rollback()
                raise _conflict(key)
            status_code, body = row.status_code, row.response_body
            await session.rollback()  # release the FOR UPDATE share
            return body, status_code, True

        if row.state == IdempotencyState.FAILED.value:
            # A leader failed while we queued: release and contest the phase-1
            # re-election (one same-payload waiter retries the business).
            await session.rollback()
            continue

        # PENDING.
        if row.request_hash != request_hash:
            await session.rollback()
            raise _conflict(key)

        if claim.leader and row.run_token == claim.run_token:
            # We own this attempt: run the business once, inside a SAVEPOINT
            # so a failure can be undone without dropping the claim-row lock
            # (other waiters stay blocked until FAILED is published).
            try:
                async with session.begin_nested():
                    status_code, body = await handler()
            except Exception:
                # SAVEPOINT rolled back: business writes are gone, the outer
                # transaction and the FOR UPDATE lock are still alive.
                row = await _lock_claim(session, scope, key)
                if (
                    row.state == IdempotencyState.PENDING.value
                    and row.run_token == claim.run_token
                ):
                    # Publish failure and release waiters in one commit.
                    row.state = IdempotencyState.FAILED.value
                    await session.commit()
                else:
                    # Ownership changed (theoretically only after a crash
                    # recovery): don't clobber the new attempt's row.
                    await session.rollback()
                # get_session's rollback() after this raise is a no-op on the
                # already-finished session.
                raise

            # SAVEPOINT released; flip to COMPLETED in the outer transaction.
            row = await _lock_claim(session, scope, key)
            if (
                row.state != IdempotencyState.PENDING.value
                or row.run_token != claim.run_token
            ):  # pragma: no cover - no other actor mutates an owned PENDING
                await session.rollback()
                continue
            row.state = IdempotencyState.COMPLETED.value
            row.request_hash = request_hash
            row.status_code = status_code
            row.response_body = jsonable_encoder(body)
            row.completed_at = utcnow()
            await session.flush()
            # The request dependency (get_session) performs the final commit,
            # publishing the business effect and the replay record atomically.
            return body, status_code, False

        # We are a waiter that grabbed the lock in the claim->lock hand-off
        # gap while the real leader has not taken it yet (or a leader whose
        # token lost the impossible-by-protocol race). Step back: release the
        # lock so the queued leader can take it, then reclassify. The leader
        # then holds the lock for the whole business run, so the next phase-2
        # attempt blocks properly instead of spinning.
        await session.rollback()
        await asyncio.sleep(_GAP_POLL_SECONDS)
        continue
