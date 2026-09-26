"""Pure tail-sampling engine.

No database, no FastAPI: given the spans belonging to one revision of a trace
and a policy spec, rebuild the trace tree, apply error propagation rules and
decide RETAIN / DISCARD, producing a full evidence trail.
"""
from __future__ import annotations

import re
from typing import Any

# ---------------------------------------------------------------------------
# Trace reconstruction
# ---------------------------------------------------------------------------


def build_trace_graph(spans: list[dict[str, Any]]) -> dict[str, Any]:
    """Rebuild a trace from potentially out-of-order spans.

    Expected span dict keys: span_id, parent_id, name, start_ms, end_ms,
    duration_ms, status, attributes, links. Handles out-of-order delivery,
    missing parents (orphans) and parent cycles.
    """
    nodes: dict[str, dict[str, Any]] = {}
    for raw in spans:
        node = dict(raw)
        node.setdefault("parent_id", None)
        node.setdefault("name", None)
        node.setdefault("status", "UNSET")
        if not node.get("attributes"):
            node["attributes"] = {}
        if not node.get("links"):
            node["links"] = []
        node["children"] = []
        node["errored"] = node["status"] == "ERROR"
        node["error_sources"] = []
        nodes[node["span_id"]] = node

    orphan_ids: set[str] = set()
    for node in nodes.values():
        pid = node["parent_id"]
        if pid is not None:
            if pid in nodes:
                nodes[pid]["children"].append(node["span_id"])
            else:
                orphan_ids.add(node["span_id"])

    cycle_span_ids = _detect_cycles(nodes)

    root_ids = [
        sid
        for sid, node in nodes.items()
        if sid not in cycle_span_ids
        and (node["parent_id"] is None or node["parent_id"] not in nodes)
    ]
    root_ids.sort(key=lambda s: (nodes[s]["start_ms"], s))
    for node in nodes.values():
        node["children"].sort(key=lambda c: (nodes[c]["start_ms"], c))

    starts = [n["start_ms"] for n in nodes.values()]
    ends = [n["end_ms"] for n in nodes.values()]
    trace_start = min(starts) if starts else 0
    trace_end = max(ends) if ends else 0

    return {
        "nodes": nodes,
        "root_ids": root_ids,
        "orphan_span_ids": sorted(orphan_ids),
        "cycle_span_ids": sorted(cycle_span_ids),
        "span_count": len(nodes),
        "trace_start_ms": trace_start,
        "trace_end_ms": trace_end,
        "trace_duration_ms": max(0, trace_end - trace_start),
    }


def _detect_cycles(nodes: dict[str, dict[str, Any]]) -> set[str]:
    """Return span ids that participate in a parent-edge cycle.

    Every node has at most one parent, so a chain is a simple walk.
    """
    UNSEEN, ACTIVE, DONE = 0, 1, 2
    state = {sid: UNSEEN for sid in nodes}
    in_cycle: set[str] = set()

    for start in nodes:
        if state[start] != UNSEEN:
            continue
        path: list[str] = []
        cur: str | None = start
        while True:
            if cur is None or cur not in nodes or state[cur] == DONE:
                for sid in path:
                    state[sid] = DONE
                break
            if state[cur] == ACTIVE:
                idx = path.index(cur)
                in_cycle.update(path[idx:])
                for sid in path:
                    state[sid] = DONE
                break
            state[cur] = ACTIVE
            path.append(cur)
            cur = nodes[cur]["parent_id"]
    return in_cycle


# ---------------------------------------------------------------------------
# Error propagation
# ---------------------------------------------------------------------------


def apply_propagation(graph: dict[str, Any], spec: dict[str, Any]) -> None:
    """Mutate graph nodes setting `errored` and `error_sources`.

    Fix-point iteration across parent/child tree edges and explicit links.
    """
    nodes = graph["nodes"]
    prop = spec.get("propagation") or {}
    to_parents = prop.get("error_to_parents", True)
    to_children = prop.get("error_to_children", False)
    link_marks = prop.get("error_link_marks", "none")

    for node in nodes.values():
        node["errored"] = node["status"] == "ERROR"
        node["error_sources"] = ["self:status=ERROR"] if node["errored"] else []

    for _ in range(len(nodes) + 2):
        changed = False
        errored = [sid for sid, n in nodes.items() if n["errored"]]
        for sid in errored:
            node = nodes[sid]

            if to_parents and node["parent_id"] in nodes:
                p = nodes[node["parent_id"]]
                if not p["errored"]:
                    p["errored"] = True
                    p["error_sources"].append(f"child_error:{sid}")
                    changed = True

            if to_children:
                for cid in node["children"]:
                    c = nodes[cid]
                    if not c["errored"]:
                        c["errored"] = True
                        c["error_sources"].append(f"parent_error:{sid}")
                        changed = True

            if link_marks == "target":
                for link in node.get("links", []):
                    tid = link["span_id"]
                    if tid in nodes and not nodes[tid]["errored"]:
                        nodes[tid]["errored"] = True
                        nodes[tid]["error_sources"].append(
                            f"linked_from_error:{sid}"
                        )
                        changed = True

        if link_marks == "source":
            # A span carrying a link descriptor marked as error is itself
            # errored; the target may live outside this trace (no node).
            for sid, node in nodes.items():
                for link in node.get("links", []):
                    if _link_is_error(link):
                        src = f"error_link:{link['span_id']}"
                        if src not in node["error_sources"]:
                            node["error_sources"].append(src)
                        if not node["errored"]:
                            node["errored"] = True
                            changed = True
                        break

        if not changed:
            break


def _link_is_error(link: dict[str, Any]) -> bool:
    return bool(link.get("is_error")) or link.get("status") == "ERROR"


# ---------------------------------------------------------------------------
# Predicate matching
# ---------------------------------------------------------------------------


def _attr_op_matches(actual: Any, op: str, expected: Any) -> bool:
    if op == "exists":
        present = actual is not _MISSING
        return present if expected is not False else not present
    if actual is _MISSING:
        return False
    try:
        if op == "eq":
            return actual == expected
        if op == "ne":
            return actual != expected
        if op == "gt":
            return actual > expected
        if op == "gte":
            return actual >= expected
        if op == "lt":
            return actual < expected
        if op == "lte":
            return actual <= expected
        if op == "in":
            return actual in (expected or [])
        if op == "nin":
            return actual not in (expected or [])
        if op == "contains":
            return expected in actual
        if op == "prefix":
            return isinstance(actual, str) and actual.startswith(expected)
    except TypeError:
        return False
    return False


_MISSING = object()


def _get_attr(attributes: dict[str, Any], key: str) -> Any:
    return attributes.get(key, _MISSING)


def _base_matches(pred: dict[str, Any], *, attributes: dict[str, Any]) -> bool:
    """Evaluate only the leaf (status/duration/name/attribute) clauses.

    For link descriptors only status and attributes exist.
    """
    status = pred.get("status")
    if status is not None and status != attributes["__status__"]:
        return False
    if pred.get("min_duration_ms") is not None:
        dur = attributes.get("__duration_ms")
        if dur is None or dur < pred["min_duration_ms"]:
            return False
    if pred.get("max_duration_ms") is not None:
        dur = attributes.get("__duration_ms")
        if dur is None or dur > pred["max_duration_ms"]:
            return False
    if pred.get("name_equals") is not None:
        if attributes.get("__name__") != pred["name_equals"]:
            return False
    if pred.get("name_matches") is not None:
        name = attributes.get("__name__")
        if not isinstance(name, str) or re.fullmatch(pred["name_matches"], name) is None:
            return False
    for clause in pred.get("attributes") or []:
        if not _attr_op_matches(
            _get_attr(attributes.get("__attrs__", {}), clause["key"]),
            clause.get("op", "eq"),
            clause.get("value"),
        ):
            return False
    return True


def _span_matches(
    pred: dict[str, Any], node: dict[str, Any], graph: dict[str, Any]
) -> bool:
    if not _base_matches(
        pred,
        attributes={
            "__status__": "ERROR" if node["errored"] else node["status"],
            "__duration_ms": node["duration_ms"],
            "__name__": node["name"],
            "__attrs__": node.get("attributes") or {},
        },
    ):
        return False

    nodes = graph["nodes"]

    parent_pred = pred.get("parent")
    if parent_pred is not None:
        pid = node["parent_id"]
        if pid not in nodes or not _span_matches(parent_pred, nodes[pid], graph):
            return False

    child_pred = pred.get("child")
    if child_pred is not None:
        if not any(
            _span_matches(child_pred, nodes[cid], graph) for cid in node["children"]
        ):
            return False

    linked_pred = pred.get("linked")
    if linked_pred is not None:
        if not any(_link_matches(linked_pred, link) for link in node.get("links", [])):
            return False

    return True


def _link_matches(pred: dict[str, Any], link: dict[str, Any]) -> bool:
    return _base_matches(
        pred,
        attributes={
            "__status__": "ERROR" if _link_is_error(link) else "UNSET",
            "__duration_ms": None,
            "__name__": None,
            "__attrs__": link.get("attributes") or {},
        },
    )


# ---------------------------------------------------------------------------
# Policy evaluation
# ---------------------------------------------------------------------------


def _candidate_ids(scope: str, graph: dict[str, Any]) -> list[str]:
    nodes = graph["nodes"]
    if scope == "root":
        ids = graph["root_ids"]
        return ids if ids else sorted(nodes, key=lambda s: (nodes[s]["start_ms"], s))
    if scope == "all":
        return sorted(nodes, key=lambda s: (nodes[s]["start_ms"], s))
    return sorted(nodes, key=lambda s: (nodes[s]["start_ms"], s))


def evaluate_policy(graph: dict[str, Any], spec: dict[str, Any]) -> dict[str, Any]:
    """Apply propagation then rules. Returns verdict + full evidence."""
    apply_propagation(graph, spec)

    rules = sorted(
        spec.get("rules") or [],
        key=lambda r: (-int(r.get("priority", 100)), r["id"]),
    )

    rule_evaluations: list[dict[str, Any]] = []
    matched_rule: dict[str, Any] | None = None

    for rule in rules:
        pred = rule["match"]
        scope = rule.get("scope", "any")
        candidates = _candidate_ids(scope, graph)
        matched_ids = [
            sid
            for sid in candidates
            if _span_matches(pred, graph["nodes"][sid], graph)
        ]
        if scope == "all":
            ok = bool(candidates) and len(matched_ids) == len(candidates)
        else:  # any / root
            ok = bool(matched_ids)
        evaluation = {
            "rule_id": rule["id"],
            "description": rule.get("description"),
            "scope": scope,
            "decision": rule["decision"],
            "priority": rule.get("priority", 100),
            "matched_span_ids": matched_ids,
            "matched": ok,
        }
        rule_evaluations.append(evaluation)
        if ok and matched_rule is None:
            matched_rule = evaluation

    default_decision = spec.get("default_decision", "DISCARD")
    verdict = matched_rule["decision"] if matched_rule else default_decision

    nodes = graph["nodes"]
    evidence = {
        "span_count": graph["span_count"],
        "trace_duration_ms": graph["trace_duration_ms"],
        "root_span_ids": graph["root_ids"],
        "orphan_span_ids": graph["orphan_span_ids"],
        "cycle_span_ids": graph["cycle_span_ids"],
        "effective_error_span_ids": sorted(
            sid for sid, n in nodes.items() if n["errored"]
        ),
        "error_sources": {
            sid: list(n["error_sources"])
            for sid, n in nodes.items()
            if n["error_sources"]
        },
        "propagation": spec.get("propagation") or {},
        "default_decision": default_decision,
        "rule_evaluations": rule_evaluations,
        "matched_rule": matched_rule,
        "verdict": verdict,
        "explanation": _explain(verdict, matched_rule, default_decision, graph),
    }
    return evidence


def _explain(
    verdict: str,
    matched_rule: dict[str, Any] | None,
    default_decision: str,
    graph: dict[str, Any],
) -> str:
    anomalies = []
    if graph["orphan_span_ids"]:
        anomalies.append(f"orphans={graph['orphan_span_ids']}")
    if graph["cycle_span_ids"]:
        anomalies.append(f"cycles={graph['cycle_span_ids']}")
    suffix = f" ({'; '.join(anomalies)})" if anomalies else ""
    if matched_rule:
        return (
            f"{verdict}: rule '{matched_rule['rule_id']}' matched "
            f"spans {matched_rule['matched_span_ids']} under scope "
            f"'{matched_rule['scope']}'{suffix}"
        )
    return f"{verdict}: no rule matched; default decision {default_decision}{suffix}"
