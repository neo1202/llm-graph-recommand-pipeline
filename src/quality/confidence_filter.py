"""Filter low-confidence tagging results for human review."""

from src.tagging.schema import TaggingOutput


def filter_low_confidence(
    result: TaggingOutput,
    threshold: float = 0.7,
) -> list[dict]:
    """
    Flag creators whose tags fall below the confidence threshold.
    Low-confidence results may indicate:
    - Taxonomy gaps (creator doesn't fit existing categories)
    - Ambiguous content (hard to classify)
    - Insufficient data (vague description, few videos)
    """
    issues = []

    # Check L1 tags
    for tag in result.l1_tags:
        if tag.confidence < threshold:
            issues.append({
                "type": "low_confidence",
                "tag": tag.tag,
                "level": "L1",
                "confidence": tag.confidence,
                "reason": f"L1 tag '{tag.tag}' has confidence {tag.confidence:.2f} "
                          f"(threshold: {threshold})",
            })

    # Check L2 tags
    for tag in result.l2_tags:
        if tag.confidence < threshold:
            issues.append({
                "type": "low_confidence",
                "tag": tag.tag,
                "level": "L2",
                "confidence": tag.confidence,
                "reason": f"L2 tag '{tag.tag}' has confidence {tag.confidence:.2f} "
                          f"(threshold: {threshold})",
            })

    # Flag if ALL tags are low confidence — likely a taxonomy gap
    all_confidences = [t.confidence for t in result.l1_tags + result.l2_tags]
    if all_confidences and max(all_confidences) < threshold:
        issues.append({
            "type": "taxonomy_gap",
            "tag": None,
            "level": "ALL",
            "confidence": max(all_confidences),
            "reason": "All tags below threshold — possible taxonomy gap. "
                      "Review this creator's content for new category needs.",
        })

    return issues
