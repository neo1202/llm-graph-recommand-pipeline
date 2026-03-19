"""Quality gate: orchestrates all validation checks before data enters the graph."""

import logging
from dataclasses import dataclass, field

from src.config import settings
from src.quality.completeness_check import check_completeness, check_hierarchy_consistency
from src.quality.confidence_filter import filter_low_confidence
from src.quality.conflict_detector import detect_conflicts, load_conflict_rules
from src.tagging.schema import TaggingOutput

logger = logging.getLogger(__name__)


@dataclass
class QualityReport:
    creator_id: str
    passed: bool
    issues: list[dict] = field(default_factory=list)
    filtered_l1_tags: list = field(default_factory=list)
    filtered_l2_tags: list = field(default_factory=list)

    @property
    def has_blocking_issues(self) -> bool:
        """Issues that prevent data from entering the graph."""
        blocking_types = {"conflict", "incomplete", "invalid_tag", "taxonomy_gap"}
        return any(i["type"] in blocking_types for i in self.issues)


class QualityGate:
    def __init__(self, taxonomy_tree: dict, taxonomy_path: str = None):
        self.taxonomy_tree = taxonomy_tree
        self.conflict_rules = load_conflict_rules(
            taxonomy_path or settings.taxonomy_path
        )
        self.confidence_threshold = settings.confidence_threshold

    def validate(self, result: TaggingOutput) -> QualityReport:
        """Run all quality checks on a tagging result."""
        all_issues = []

        # 1. Completeness check
        all_issues.extend(check_completeness(result))

        # 2. Hierarchy consistency (catches hallucinated tags)
        all_issues.extend(
            check_hierarchy_consistency(result, self.taxonomy_tree)
        )

        # 3. Conflict detection
        all_tag_names = [t.tag for t in result.l1_tags + result.l2_tags]
        all_issues.extend(
            detect_conflicts(all_tag_names, self.conflict_rules)
        )

        # 4. Confidence filtering
        all_issues.extend(
            filter_low_confidence(result, self.confidence_threshold)
        )

        # Filter out invalid/hallucinated tags from the result
        valid_l2_names = set()
        for l1_data in self.taxonomy_tree.values():
            valid_l2_names.update(l1_data.get("children", {}).keys())
        valid_l1_names = set(self.taxonomy_tree.keys())

        filtered_l1 = [t for t in result.l1_tags if t.tag in valid_l1_names]
        filtered_l2 = [t for t in result.l2_tags if t.tag in valid_l2_names]

        # Remove conflicting tags (keep the one with higher confidence)
        conflict_tags_to_remove = set()
        for issue in all_issues:
            if issue["type"] == "conflict":
                tag_a, tag_b = issue["tags"]
                conf_a = next(
                    (t.confidence for t in result.l2_tags if t.tag == tag_a), 0
                )
                conf_b = next(
                    (t.confidence for t in result.l2_tags if t.tag == tag_b), 0
                )
                # Remove the lower confidence one
                conflict_tags_to_remove.add(tag_a if conf_a < conf_b else tag_b)

        filtered_l2 = [
            t for t in filtered_l2 if t.tag not in conflict_tags_to_remove
        ]

        passed = not any(
            i["type"] in {"incomplete", "invalid_tag"} for i in all_issues
        )

        report = QualityReport(
            creator_id=result.creator_id,
            passed=passed,
            issues=all_issues,
            filtered_l1_tags=filtered_l1,
            filtered_l2_tags=filtered_l2,
        )

        if all_issues:
            logger.warning(
                f"Quality issues for {result.creator_id}: "
                f"{len(all_issues)} issue(s) — "
                f"blocking={report.has_blocking_issues}"
            )

        return report
