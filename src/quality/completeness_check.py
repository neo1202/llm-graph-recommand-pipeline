"""Ensure every creator has at least minimum required tags."""

from src.tagging.schema import TaggingOutput


def check_completeness(result: TaggingOutput) -> list[dict]:
    """
    Verify that tagging output meets minimum requirements:
    - At least 1 L1 tag
    - At least 1 L2 tag
    - L2 tags should belong to predicted L1 categories
    """
    issues = []

    if not result.l1_tags:
        issues.append({
            "type": "incomplete",
            "reason": "No L1 categories assigned. Creator must have at least one broad category.",
        })

    if not result.l2_tags:
        issues.append({
            "type": "incomplete",
            "reason": "No L2 tags assigned. Creator must have at least one specific tag.",
        })

    return issues


def check_hierarchy_consistency(
    result: TaggingOutput,
    taxonomy_tree: dict,
) -> list[dict]:
    """
    Verify L2 tags actually belong to the predicted L1 categories.
    Catches LLM hallucinations where it assigns L2 tags from wrong parents.
    """
    issues = []
    l1_names = {t.tag for t in result.l1_tags}

    # Build reverse mapping: L2 tag -> L1 parent
    l2_to_l1 = {}
    for l1_name, l1_data in taxonomy_tree.items():
        for l2_name in l1_data.get("children", {}):
            l2_to_l1[l2_name] = l1_name

    for tag in result.l2_tags:
        parent = l2_to_l1.get(tag.tag)
        if parent is None:
            issues.append({
                "type": "invalid_tag",
                "tag": tag.tag,
                "reason": f"L2 tag '{tag.tag}' does not exist in taxonomy. "
                          "Possible LLM hallucination.",
            })
        elif parent not in l1_names:
            issues.append({
                "type": "hierarchy_mismatch",
                "tag": tag.tag,
                "expected_parent": parent,
                "actual_l1s": list(l1_names),
                "reason": f"L2 tag '{tag.tag}' belongs to '{parent}' but "
                          f"predicted L1 categories are {list(l1_names)}.",
            })

    return issues
