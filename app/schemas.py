"""Pydantic schemas: ingestion API models and the policy rule DSL."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator

# ---------------------------------------------------------------------------
# Span ingestion
# ---------------------------------------------------------------------------

SpanStatus = Literal["OK", "ERROR", "UNSET"]


def _as_utc(dt: datetime) -> datetime:
    """Normalize to timezone-aware UTC (asyncpg timestamptz rejects naive)."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


class LinkIn(BaseModel):
    trace_id: str | None = None
    span_id: str
    attributes: dict[str, Any] = Field(default_factory=dict)
    # Explicitly marks this link as pointing at an errored span.
    is_error: bool = False


class SpanIn(BaseModel):
    span_id: str
    parent_id: str | None = None
    name: str | None = None
    start_time: datetime
    end_time: datetime | None = None
    duration_ms: int | None = None
    status: SpanStatus = "UNSET"
    attributes: dict[str, Any] = Field(default_factory=dict)
    links: list[LinkIn] = Field(default_factory=list)

    @model_validator(mode="after")
    def _complete_timing(self) -> "SpanIn":
        self.start_time = _as_utc(self.start_time)
        if self.end_time is not None:
            self.end_time = _as_utc(self.end_time)
        if self.end_time is None and self.duration_ms is None:
            raise ValueError("either end_time or duration_ms is required")
        if self.end_time is None:
            self.end_time = self.start_time + timedelta(
                milliseconds=self.duration_ms
            )
        if self.end_time < self.start_time:
            raise ValueError("end_time must be >= start_time")
        if self.duration_ms is None:
            self.duration_ms = int(
                (self.end_time - self.start_time).total_seconds() * 1000
            )
        return self


class BatchIngestIn(BaseModel):
    spans: list[SpanIn]

    @model_validator(mode="after")
    def _unique_span_ids(self) -> "BatchIngestIn":
        ids = [s.span_id for s in self.spans]
        dupes = {sid for sid in ids if ids.count(sid) > 1}
        if dupes:
            raise ValueError(f"duplicate span_id within batch: {sorted(dupes)}")
        return self


class IngestResult(BaseModel):
    span_id: str
    outcome: Literal["stored", "duplicate"]


class IngestResponse(BaseModel):
    trace_id: str
    revision: int
    state: str
    received: int
    stored: int
    duplicates: int
    results: list[IngestResult]
    note: str | None = None


# ---------------------------------------------------------------------------
# Policy rule DSL
# ---------------------------------------------------------------------------

AttrOp = Literal[
    "eq", "ne", "gt", "gte", "lt", "lte", "in", "nin", "exists", "contains", "prefix"
]
Scope = Literal["any", "all", "root"]
RuleDecision = Literal["RETAIN", "DISCARD"]


class AttrField(BaseModel):
    key: str
    op: AttrOp = "eq"
    value: Any | None = None


class Predicate(BaseModel):
    """Matches a span. Nested parent/child/linked predicates express
    parent-child and link propagation requirements."""

    status: Literal["ERROR", "OK", "UNSET"] | None = None
    min_duration_ms: int | None = None
    max_duration_ms: int | None = None
    name_equals: str | None = None
    name_matches: str | None = None  # Python regex, fullmatch
    attributes: list[AttrField] = Field(default_factory=list)
    parent: "Predicate | None" = None
    child: "Predicate | None" = None
    # Evaluated against the link descriptors carried by the span. Only
    # status (= link.is_error) and attributes are available for links.
    linked: "Predicate | None" = None


class Rule(BaseModel):
    id: str
    description: str | None = None
    scope: Scope = "any"
    match: Predicate
    decision: RuleDecision
    priority: int = 100


class Propagation(BaseModel):
    # Error status inherited by ancestors / descendants through the tree.
    error_to_parents: bool = True
    error_to_children: bool = False
    # Links: "source" marks a span that links to an errored span,
    # "target" marks a span linked from an errored span.
    error_link_marks: Literal["none", "source", "target"] = "none"


class PolicySpec(BaseModel):
    default_decision: RuleDecision = "DISCARD"
    propagation: Propagation = Field(default_factory=Propagation)
    rules: list[Rule] = Field(default_factory=list)

    @model_validator(mode="after")
    def _unique_rule_ids(self) -> "PolicySpec":
        ids = [r.id for r in self.rules]
        dupes = {rid for rid in ids if ids.count(rid) > 1}
        if dupes:
            raise ValueError(f"duplicate rule ids: {sorted(dupes)}")
        return self


Predicate.model_rebuild()


# ---------------------------------------------------------------------------
# Policies API
# ---------------------------------------------------------------------------


class PublishPolicyIn(BaseModel):
    name: str
    spec: PolicySpec
    expected_version: int | None = Field(
        default=None,
        description="CAS: must equal the current active version; null publishes only if none exists",
    )
    created_by: str | None = None


class PolicyOut(BaseModel):
    version: int
    name: str
    spec: dict[str, Any]
    is_active: bool
    created_at: datetime
    created_by: str | None = None


# ---------------------------------------------------------------------------
# Read models
# ---------------------------------------------------------------------------


class LinkOut(BaseModel):
    trace_id: str | None
    span_id: str
    attributes: dict[str, Any]
    is_error: bool


class SpanOut(BaseModel):
    span_id: str
    parent_id: str | None
    name: str | None
    start_time: datetime
    end_time: datetime
    duration_ms: int
    status: str
    attributes: dict[str, Any]
    links: list[LinkOut]
    revision: int


class DecisionSummary(BaseModel):
    id: int
    revision: int
    policy_version: int
    verdict: str
    close_reason: str
    span_count: int
    evidence: dict[str, Any]
    created_at: datetime


class TraceOut(BaseModel):
    trace_id: str
    state: str
    revision: int
    policy_version: int | None
    current_decision_id: int | None
    first_seen_at: datetime
    last_span_at: datetime
    revision_started_at: datetime


class TraceDetailOut(TraceOut):
    spans: list[SpanOut]


class FinalizeResponse(BaseModel):
    trace: TraceOut
    decision: DecisionSummary | None = None
    note: str | None = None


class ReplayResponse(BaseModel):
    trace_id: str
    revision: int
    requested_policy_version: int
    policy_version: int
    verdict: str
    close_reason: str
    span_count: int
    evidence: dict[str, Any]
    stored_verdict: str | None = None
    matches_published: bool | None = None
