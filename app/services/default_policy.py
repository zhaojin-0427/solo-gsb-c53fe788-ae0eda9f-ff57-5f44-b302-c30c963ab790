"""Built-in v1 policy seeded on first boot."""
from __future__ import annotations

DEFAULT_POLICY_NAME = "default-v1"

DEFAULT_POLICY_SPEC: dict = {
    "default_decision": "DISCARD",
    "propagation": {
        "error_to_parents": True,
        "error_to_children": False,
        "error_link_marks": "source",
    },
    "rules": [
        {
            "id": "error-any",
            "description": "Retain any trace containing an error (errors propagate to parents).",
            "scope": "any",
            "priority": 200,
            "decision": "RETAIN",
            "match": {"status": "ERROR"},
        },
        {
            "id": "slow-any",
            "description": "Retain any trace containing a span slower than 1s.",
            "scope": "any",
            "priority": 100,
            "decision": "RETAIN",
            "match": {"min_duration_ms": 1000},
        },
    ],
}
