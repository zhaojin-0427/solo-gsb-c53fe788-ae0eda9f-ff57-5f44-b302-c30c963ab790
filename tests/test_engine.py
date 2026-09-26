"""Unit tests for the pure sampling engine: no database required."""
from __future__ import annotations

from app.sampling.engine import build_trace_graph, evaluate_policy

T0 = 1_000_000  # arbitrary epoch ms


def span(
    span_id,
    parent_id=None,
    status="OK",
    duration_ms=10,
    start_ms=None,
    name="op",
    attributes=None,
    links=None,
):
    start = T0 if start_ms is None else start_ms
    return {
        "span_id": span_id,
        "parent_id": parent_id,
        "name": name,
        "start_ms": start,
        "end_ms": start + duration_ms,
        "duration_ms": duration_ms,
        "status": status,
        "attributes": attributes or {},
        "links": links or [],
    }


POLICY_ERROR_SLOW = {
    "default_decision": "DISCARD",
    "propagation": {"error_to_parents": True, "error_to_children": False},
    "rules": [
        {"id": "err", "scope": "any", "priority": 200, "decision": "RETAIN",
         "match": {"status": "ERROR"}},
        {"id": "slow", "scope": "any", "priority": 100, "decision": "RETAIN",
         "match": {"min_duration_ms": 1000}},
    ],
}


def test_out_of_order_rebuild_sets_roots_and_children():
    # Child reported before parent.
    graph = build_trace_graph([span("child", "root"), span("root", None)])
    assert graph["root_ids"] == ["root"]
    assert graph["nodes"]["root"]["children"] == ["child"]
    assert graph["orphan_span_ids"] == []


def test_orphan_spans_are_reported():
    graph = build_trace_graph([span("a", "missing-parent")])
    assert graph["orphan_span_ids"] == ["a"]
    # An orphan acts as a root for rule evaluation.
    assert graph["root_ids"] == ["a"]


def test_parent_cycle_is_detected_and_not_rooted():
    graph = build_trace_graph([span("a", "b"), span("b", "a")])
    assert set(graph["cycle_span_ids"]) == {"a", "b"}
    assert graph["root_ids"] == []


def test_clean_trace_is_discarded_by_default():
    graph = build_trace_graph([span("root"), span("c1", "root")])
    ev = evaluate_policy(graph, POLICY_ERROR_SLOW)
    assert ev["verdict"] == "DISCARD"
    assert ev["matched_rule"] is None


def test_error_anywhere_retains_via_parent_propagation():
    # Root is OK, grandchild is ERROR: propagation marks the whole chain.
    graph = build_trace_graph(
        [span("root", None, status="OK"),
         span("mid", "root", status="OK"),
         span("leaf", "mid", status="ERROR")]
    )
    ev = evaluate_policy(graph, POLICY_ERROR_SLOW)
    assert ev["verdict"] == "RETAIN"
    assert ev["matched_rule"]["rule_id"] == "err"
    assert set(ev["effective_error_span_ids"]) == {"root", "mid", "leaf"}
    assert "child_error:leaf" in ev["error_sources"]["mid"]


def test_error_does_not_propagate_to_children_by_default():
    graph = build_trace_graph(
        [span("root", None, status="ERROR"), span("child", "root", status="OK")]
    )
    ev = evaluate_policy(graph, POLICY_ERROR_SLOW)
    # root itself is ERROR so RETAIN anyway, but child stays clean
    assert "child" not in ev["effective_error_span_ids"]


def test_error_to_children_propagation():
    policy = {
        "default_decision": "DISCARD",
        "propagation": {"error_to_parents": False, "error_to_children": True},
        "rules": [
            {"id": "err", "scope": "any", "decision": "RETAIN",
             "match": {"status": "ERROR"}}
        ],
    }
    graph = build_trace_graph(
        [span("root", None, status="ERROR"), span("child", "root", status="OK")]
    )
    ev = evaluate_policy(graph, policy)
    assert set(ev["effective_error_span_ids"]) == {"root", "child"}


def test_latency_rule_retains_slow_span():
    graph = build_trace_graph([span("root", duration_ms=1500)])
    ev = evaluate_policy(graph, POLICY_ERROR_SLOW)
    assert ev["verdict"] == "RETAIN"
    assert ev["matched_rule"]["rule_id"] == "slow"


def test_higher_priority_wins_and_discard_rule_can_win_by_priority():
    policy = {
        "default_decision": "RETAIN",
        "propagation": {},
        "rules": [
            {"id": "healthz-discard", "scope": "root", "priority": 300,
             "decision": "DISCARD", "match": {"name_equals": "healthz"}},
            {"id": "err-retain", "scope": "any", "priority": 100,
             "decision": "RETAIN", "match": {"status": "ERROR"}},
        ],
    }
    graph = build_trace_graph(
        [span("root", None, name="healthz"), span("c", "root", status="ERROR")]
    )
    ev = evaluate_policy(graph, policy)
    assert ev["verdict"] == "DISCARD"
    assert ev["matched_rule"]["rule_id"] == "healthz-discard"


def test_scope_all_requires_every_span():
    policy = {
        "default_decision": "DISCARD",
        "propagation": {},
        "rules": [
            {"id": "all-slow", "scope": "all", "decision": "RETAIN",
             "match": {"min_duration_ms": 100}}
        ],
    }
    g1 = build_trace_graph([span("a", duration_ms=200), span("b", "a", duration_ms=200)])
    assert evaluate_policy(g1, policy)["verdict"] == "RETAIN"
    g2 = build_trace_graph([span("a", duration_ms=200), span("b", "a", duration_ms=50)])
    ev2 = evaluate_policy(g2, policy)
    assert ev2["verdict"] == "DISCARD"
    assert ev2["rule_evaluations"][0]["matched_span_ids"] == ["a"]


def test_attribute_combo_rule():
    policy = {
        "default_decision": "DISCARD",
        "propagation": {},
        "rules": [
            {"id": "combo", "scope": "any", "decision": "RETAIN",
             "match": {"attributes": [
                 {"key": "service", "op": "eq", "value": "payments"},
                 {"key": "http.route", "op": "prefix", "value": "/checkout"},
                 {"key": "retry.count", "op": "gte", "value": 2},
             ]}}
        ],
    }
    g = build_trace_graph([
        span("root", attributes={"service": "payments",
                                 "http.route": "/checkout/123",
                                 "retry.count": 3}),
        span("c", "root", attributes={"service": "payments",
                                      "http.route": "/health",
                                      "retry.count": 0}),
    ])
    ev = evaluate_policy(g, policy)
    assert ev["verdict"] == "RETAIN"
    assert ev["matched_rule"]["matched_span_ids"] == ["root"]


def test_parent_child_predicate_combo():
    # Retain when an errored child runs under a parent tagged canary.
    policy = {
        "default_decision": "DISCARD",
        "propagation": {"error_to_parents": False, "error_to_children": False},
        "rules": [
            {"id": "canary-child-error", "scope": "any", "decision": "RETAIN",
             "match": {
                 "status": "ERROR",
                 "parent": {"attributes": [
                     {"key": "deployment", "value": "canary"}]},
             }}
        ],
    }
    g = build_trace_graph([
        span("p", None, attributes={"deployment": "canary"}),
        span("c", "p", status="ERROR"),
    ])
    ev = evaluate_policy(g, policy)
    assert ev["verdict"] == "RETAIN"
    assert ev["matched_rule"]["matched_span_ids"] == ["c"]

    g2 = build_trace_graph([
        span("p", None, attributes={"deployment": "stable"}),
        span("c", "p", status="ERROR"),
    ])
    assert evaluate_policy(g2, policy)["verdict"] == "DISCARD"


def test_child_predicate_on_parent():
    policy = {
        "default_decision": "DISCARD",
        "propagation": {},
        "rules": [
            {"id": "root-with-slow-child", "scope": "root", "decision": "RETAIN",
             "match": {"child": {"min_duration_ms": 500}}}
        ],
    }
    g = build_trace_graph([span("r", None), span("c", "r", duration_ms=600)])
    assert evaluate_policy(g, policy)["verdict"] == "RETAIN"


def test_linked_error_source_propagation():
    policy = {
        "default_decision": "DISCARD",
        "propagation": {"error_to_parents": True, "error_link_marks": "source"},
        "rules": [
            {"id": "linked-err", "scope": "any", "decision": "RETAIN",
             "match": {"linked": {"status": "ERROR"}}}
        ],
    }
    g = build_trace_graph([
        span("r", None, links=[{"span_id": "other", "is_error": True,
                                "attributes": {"kind": "follows_from"}}])
    ])
    ev = evaluate_policy(g, policy)
    assert ev["verdict"] == "RETAIN"
    # propagation marks the span itself as effective-errored via the link
    assert "error_link:other" in ev["error_sources"]["r"]


def test_linked_attribute_predicate():
    policy = {
        "default_decision": "DISCARD",
        "propagation": {"error_to_parents": True, "error_link_marks": "none"},
        "rules": [
            {"id": "linked-batch", "scope": "any", "decision": "RETAIN",
             "match": {"linked": {"attributes": [
                 {"key": "messaging.system", "value": "kafka"}]}}}
        ],
    }
    g = build_trace_graph([
        span("r", None, links=[{"span_id": "x", "attributes": {
            "messaging.system": "kafka"}}])
    ])
    assert evaluate_policy(g, policy)["verdict"] == "RETAIN"


def test_evidence_contains_rule_by_rule_evaluation():
    g = build_trace_graph([span("root"), span("c", "root", duration_ms=2000)])
    ev = evaluate_policy(g, POLICY_ERROR_SLOW)
    ids = [r["rule_id"] for r in ev["rule_evaluations"]]
    assert ids == ["err", "slow"]  # priority order
    assert ev["rule_evaluations"][1]["matched_span_ids"] == ["c"]
    assert "slow" in ev["explanation"]
