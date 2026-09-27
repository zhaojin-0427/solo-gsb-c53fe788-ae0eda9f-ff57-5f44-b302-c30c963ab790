"""SQLAlchemy ORM models."""
from __future__ import annotations

import enum
from datetime import datetime, timezone

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class TraceState(str, enum.Enum):
    OPEN = "OPEN"
    ARCHIVED = "ARCHIVED"  # a decision (RETAIN or DISCARD) was published


class Verdict(str, enum.Enum):
    RETAIN = "RETAIN"
    DISCARD = "DISCARD"
    INDETERMINATE = "INDETERMINATE"  # un-closed trace crossed the hard watermark


class CloseReason(str, enum.Enum):
    WINDOW = "WINDOW"
    END_SIGNAL = "END_SIGNAL"
    WATERMARK = "WATERMARK"


class Trace(Base):
    __tablename__ = "traces"

    trace_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    state: Mapped[str] = mapped_column(String(16), default=TraceState.OPEN.value)
    revision: Mapped[int] = mapped_column(Integer, default=0)
    policy_version: Mapped[int | None] = mapped_column(Integer, nullable=True)
    current_decision_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    first_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow
    )
    # Wall clock of the most recent accepted span; idle window closes relative
    # to this timestamp.
    last_span_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow
    )
    # Start clock of the current revision (revision 1 starts at first_seen_at);
    # the hard watermark closes relative to this timestamp.
    revision_started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow
    )

    spans: Mapped[list[Span]] = relationship(
        back_populates="trace", cascade="all, delete-orphan"
    )
    decisions: Mapped[list[Decision]] = relationship(
        back_populates="trace", cascade="all, delete-orphan"
    )


class Span(Base):
    __tablename__ = "spans"
    __table_args__ = (
        UniqueConstraint("trace_id", "span_id", name="uq_span_trace_span"),
        Index("ix_spans_trace_revision", "trace_id", "revision"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    trace_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("traces.trace_id", ondelete="CASCADE"), index=True
    )
    span_id: Mapped[str] = mapped_column(String(128), index=True)
    parent_id: Mapped[str | None] = mapped_column(String(128), nullable=True)

    name: Mapped[str | None] = mapped_column(Text, nullable=True)
    start_time: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    end_time: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    duration_ms: Mapped[int] = mapped_column(BigInteger)

    status: Mapped[str] = mapped_column(String(16), default="UNSET")
    attributes: Mapped[dict] = mapped_column(JSONB, default=dict)
    links: Mapped[list] = mapped_column(JSONB, default=list)

    # Revision on which this span first arrived.
    revision: Mapped[int] = mapped_column(Integer, default=1)
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow
    )

    trace: Mapped[Trace] = relationship(back_populates="spans")


class Policy(Base):
    __tablename__ = "policies"

    version: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(Text)
    spec: Mapped[dict] = mapped_column(JSONB)
    is_active: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow
    )
    created_by: Mapped[str | None] = mapped_column(Text, nullable=True)


class Decision(Base):
    __tablename__ = "decisions"
    # At most one published decision per (trace, revision): the DB unique
    # constraint plus trace-row locking guarantee concurrent finalizers can
    # never archive the same revision twice.
    __table_args__ = (
        UniqueConstraint("trace_id", "revision", name="uq_decision_trace_revision"),
        Index("ix_decisions_verdict", "verdict"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    trace_id: Mapped[str] = mapped_column(
        String(128), ForeignKey("traces.trace_id", ondelete="CASCADE"), index=True
    )
    revision: Mapped[int] = mapped_column(Integer)
    policy_version: Mapped[int]
    verdict: Mapped[str] = mapped_column(String(16))
    close_reason: Mapped[str] = mapped_column(String(16))
    span_count: Mapped[int] = mapped_column(Integer)
    evidence: Mapped[dict] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow
    )

    trace: Mapped[Trace] = relationship(back_populates="decisions")


class IdempotencyState(str, enum.Enum):
    # Claim is published; the leader holds the row's FOR UPDATE lock while
    # running the business logic inside a separate transaction.
    PENDING = "PENDING"
    # The leader committed; status_code/response_body are authoritative.
    COMPLETED = "COMPLETED"
    # The leader's business transaction rolled back. The claim stays in place
    # so waiters can re-elect one of themselves (bounded retries); once nobody
    # is waiting, a fresh (possibly corrected) request claims it immediately.
    FAILED = "FAILED"


class IdempotencyKey(Base):
    """Idempotency-Key ledger.

    The claim is created atomically (``INSERT ... ON CONFLICT DO NOTHING``)
    in the PENDING state and **committed in its own short transaction before
    any business write runs**, so the placeholder -- including the request
    fingerprint -- is immediately visible to concurrent peers: a same-key
    different-payload request returns 409 right away without waiting for the
    leader's business transaction. The leader then opens its business
    transaction and takes a ``SELECT ... FOR UPDATE`` lock on the claim row
    for the duration of the business work. Same-payload peers block on that
    lock and replay the stored COMPLETED response (identical status/body);
    different-payload peers never lock and never block.

    If the leader's business work raises, a SAVEPOINT rollback removes every
    business write while the claim-row lock is retained; the row is flipped
    to FAILED and committed in that same transaction, atomically releasing
    the waiting peers (one of whom wins an UPDATE election back to PENDING
    and retries the business) -- the business logic runs at most once per
    *commit*, never producing a unique-constraint 500. A leader that dies
    hard leaves a PENDING row whose lock is freely acquirable; after
    ``TS_IDEMPOTENCY_PENDING_TTL_MS`` such a stale placeholder is taken over
    (attempts bumped for observability). The takeover probe re-reads the row
    under the FOR UPDATE lock it acquires (``populate_existing``), so a row
    that COMPLETED while the peer was classifying is never reclaimed: its
    fingerprint, status code and response body stay the first writer's.
    Symmetrically, when the probe finds the row lock held by a live leader,
    the same/different-payload decision is re-judged against the CURRENT
    committed owner's fingerprint (fresh ``populate_existing`` re-read): a
    peer may have reclaimed the expired placeholder -- committing a new
    fingerprint -- and become the lock holder after our first unlocked read,
    so judging on that stale snapshot would wrongly refuse a request that
    carries the new owner's exact payload.
    """

    __tablename__ = "idempotency_keys"
    __table_args__ = (
        UniqueConstraint("scope", "key", name="uq_idempotency_scope_key"),
        Index("ix_idempotency_state", "state"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    scope: Mapped[str] = mapped_column(String(160))
    key: Mapped[str] = mapped_column(String(256))
    # SHA-256 fingerprint of the canonical request payload.
    request_hash: Mapped[str] = mapped_column(String(64))
    state: Mapped[str] = mapped_column(
        String(16), default=IdempotencyState.PENDING.value
    )
    # NULL while PENDING/FAILED; the exact status/body every follower replays.
    response_body: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    status_code: Mapped[int | None] = mapped_column(Integer, nullable=True)
    # Unique-per-attempt tag of the current leader. Claim INSERTed this token;
    # a wait/retry loop re-reads the row under FOR UPDATE and only proceeds as
    # leader while the token still matches -- otherwise someone else won the
    # (re-)election and the loop attaches to the new owner instead.
    run_token: Mapped[str | None] = mapped_column(String(36), nullable=True)
    # How many leaders owned this placeholder (stale takeovers included).
    attempts: Mapped[int] = mapped_column(Integer, default=1)
    locked_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow
    )
