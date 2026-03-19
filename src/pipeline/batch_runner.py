"""
Batch pipeline runner: processes creators end-to-end.
Ingestion → LLM Tagging → Quality Gates → Storage (PostgreSQL staging)
Neo4j writes are deferred until review approval via the dashboard.
"""

import json
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path

from src.config import settings
from src.graph.neo4j_client import get_neo4j_client
from src.graph.taxonomy_loader import get_taxonomy_tree, init_taxonomy_graph
from src.quality.gate import QualityGate
from src.storage.models import AuditLog, Creator, ReviewQueue, TaggingResult, TagSuggestionLog
from src.storage.postgres_client import get_session, init_db
from src.tagging.llm_tagger import LLMTagger
from src.tagging.schema import CreatorInput

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


@dataclass
class PipelineStats:
    total: int = 0
    processed: int = 0
    passed: int = 0
    flagged: int = 0
    failed: int = 0
    total_issues: int = 0
    total_time_seconds: float = 0.0
    avg_confidence_l1: float = 0.0
    avg_confidence_l2: float = 0.0
    _l1_confidences: list = field(default_factory=list)
    _l2_confidences: list = field(default_factory=list)

    def finalize(self):
        if self._l1_confidences:
            self.avg_confidence_l1 = sum(self._l1_confidences) / len(self._l1_confidences)
        if self._l2_confidences:
            self.avg_confidence_l2 = sum(self._l2_confidences) / len(self._l2_confidences)

    def summary(self) -> str:
        return (
            f"\n{'='*60}\n"
            f"Pipeline Summary\n"
            f"{'='*60}\n"
            f"Total creators:    {self.total}\n"
            f"Processed:         {self.processed}\n"
            f"Passed QA:         {self.passed}\n"
            f"Flagged for review:{self.flagged}\n"
            f"Failed:            {self.failed}\n"
            f"Total issues:      {self.total_issues}\n"
            f"Avg L1 confidence: {self.avg_confidence_l1:.3f}\n"
            f"Avg L2 confidence: {self.avg_confidence_l2:.3f}\n"
            f"Total time:        {self.total_time_seconds:.1f}s\n"
            f"Avg per creator:   {self.total_time_seconds/max(self.processed,1):.1f}s\n"
            f"{'='*60}"
        )


def load_creators(data_path: str) -> list[dict]:
    """Load creator data from JSON file."""
    with open(data_path) as f:
        return json.load(f)


def load_few_shot_examples(path: str) -> list[dict]:
    """Load few-shot examples for LLM prompting."""
    try:
        with open(path) as f:
            return json.load(f)
    except FileNotFoundError:
        logger.warning(f"Few-shot examples not found at {path}")
        return []


def run_pipeline(
    data_path: str = "data/seed_creators.json",
    limit: int | None = None,
):
    """Run the full batch tagging pipeline."""
    start_time = time.time()

    # Initialize infrastructure
    logger.info("Initializing databases...")
    init_db()
    neo4j = get_neo4j_client()

    # Load and initialize taxonomy
    logger.info("Loading taxonomy into Neo4j...")
    taxonomy_stats = init_taxonomy_graph(neo4j, settings.taxonomy_path)
    logger.info(f"Taxonomy loaded: {taxonomy_stats}")
    taxonomy_tree = get_taxonomy_tree(neo4j)

    # Initialize components
    few_shots = load_few_shot_examples(settings.few_shot_path)
    tagger = LLMTagger(taxonomy_tree, few_shots)
    quality_gate = QualityGate(taxonomy_tree)

    # Load creator data
    creators_data = load_creators(data_path)
    if limit:
        creators_data = creators_data[:limit]

    stats = PipelineStats(total=len(creators_data))
    logger.info(f"Processing {stats.total} creators...")

    session = get_session()

    try:
        for i, raw in enumerate(creators_data):
            creator_input = CreatorInput(
                channel_id=raw["channel_id"],
                name=raw["name"],
                description=raw["description"],
                recent_video_titles=raw["recent_video_titles"],
                subscriber_count=raw.get("subscriber_count", 0),
                region=raw.get("region", "Global"),
            )

            try:
                # Skip if already processed
                existing = session.query(Creator).filter_by(
                    channel_id=creator_input.channel_id
                ).first()
                if existing:
                    stats.processed += 1
                    stats.passed += 1
                    logger.info(f"Skipping {creator_input.name} (already exists)")
                    continue

                # Step 1: LLM Tagging (2-stage)
                tagging_result = tagger.tag_creator(creator_input)

                # Step 2: Quality Gates
                qa_report = quality_gate.validate(tagging_result)
                stats.total_issues += len(qa_report.issues)

                # Step 3: Store in PostgreSQL
                db_creator = session.query(Creator).filter_by(
                    channel_id=creator_input.channel_id
                ).first()
                if not db_creator:
                    db_creator = Creator(
                        channel_id=creator_input.channel_id,
                        name=creator_input.name,
                        description=creator_input.description,
                        subscriber_count=creator_input.subscriber_count,
                        region=creator_input.region,
                        video_titles=json.dumps(creator_input.recent_video_titles),
                    )
                    session.add(db_creator)
                    session.flush()

                # Store tagging results
                for tag in qa_report.filtered_l1_tags + qa_report.filtered_l2_tags:
                    level = "L1" if tag in qa_report.filtered_l1_tags else "L2"
                    session.add(TaggingResult(
                        creator_id=db_creator.id,
                        tag_name=tag.tag,
                        tag_level=level,
                        confidence=tag.confidence,
                    ))

                # Store LLM suggested new tags
                for suggestion in tagging_result.suggested_new_tags:
                    session.add(TagSuggestionLog(
                        creator_id=db_creator.id,
                        suggested_tag=suggestion.suggested_tag,
                        parent_l1=suggestion.parent_l1,
                        reason=suggestion.reason,
                    ))

                # Step 4: Every creator enters review queue (Neo4j write deferred until approval)
                has_issues = len(qa_report.issues) > 0
                if has_issues:
                    stats.flagged += 1
                else:
                    stats.passed += 1

                session.add(ReviewQueue(
                    creator_id=db_creator.id,
                    reason="flagged" if has_issues else "auto_pass",
                    details=json.dumps(qa_report.issues) if has_issues else "[]",
                ))

                session.add(AuditLog(
                    creator_id=db_creator.id,
                    action="tagged",
                    details=json.dumps({
                        "l1": [t.tag for t in qa_report.filtered_l1_tags],
                        "l2": [t.tag for t in qa_report.filtered_l2_tags],
                    }),
                ))

                # Track confidence scores
                stats._l1_confidences.extend(
                    t.confidence for t in qa_report.filtered_l1_tags
                )
                stats._l2_confidences.extend(
                    t.confidence for t in qa_report.filtered_l2_tags
                )
                stats.processed += 1

                if (i + 1) % 10 == 0:
                    session.commit()
                    logger.info(f"Progress: {i+1}/{stats.total}")

            except Exception as e:
                stats.failed += 1
                logger.error(f"Failed to process {creator_input.name}: {e}")
                session.add(AuditLog(
                    creator_id=None,
                    action="error",
                    details=json.dumps({
                        "channel_id": creator_input.channel_id,
                        "error": str(e),
                    }),
                ))

        session.commit()

    finally:
        session.close()

    stats.total_time_seconds = time.time() - start_time
    stats.finalize()
    logger.info(stats.summary())

    return stats


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run iKala tagging pipeline")
    parser.add_argument("--data", default="data/seed_creators.json", help="Path to creator data")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of creators to process")
    args = parser.parse_args()

    run_pipeline(data_path=args.data, limit=args.limit)
