"""Detect logically conflicting tag combinations."""

import yaml


def load_conflict_rules(taxonomy_path: str) -> list[tuple[str, str]]:
    """Load conflict rules from taxonomy YAML."""
    with open(taxonomy_path) as f:
        taxonomy = yaml.safe_load(f)
    return [tuple(pair) for pair in taxonomy.get("conflict_rules", [])]


def detect_conflicts(
    tag_names: list[str],
    conflict_rules: list[tuple[str, str]],
) -> list[dict]:
    """
    Check if any assigned tags violate conflict rules.
    Returns list of conflict details.
    """
    tag_set = set(tag_names)
    conflicts = []

    for tag_a, tag_b in conflict_rules:
        if tag_a in tag_set and tag_b in tag_set:
            conflicts.append({
                "type": "conflict",
                "tags": [tag_a, tag_b],
                "reason": f"Tags '{tag_a}' and '{tag_b}' cannot coexist on the same creator",
            })

    return conflicts
