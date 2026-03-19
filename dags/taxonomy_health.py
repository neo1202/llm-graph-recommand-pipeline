"""
DAG: taxonomy_health
Runs weekly — analyzes tag distribution, detects taxonomy gaps, and surfaces
new tag suggestions from LLM outputs.

Taxonomy gap signals:
1. L1 assigned but no L2 matched → L2 coverage is incomplete under that L1
2. LLM suggested_new_tags accumulated from tagging pipeline
3. Tags with too many or too few creators (too broad / too narrow)
"""

import json
import logging
from collections import Counter, defaultdict
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

default_args = {
    "owner": "ikala-data",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def task_tag_distribution(**context):
    """Analyze how tags are distributed across creators."""
    from src.storage.models import TaggingResult
    from src.storage.postgres_client import get_session, init_db

    init_db()
    session = get_session()
    try:
        results = session.query(TaggingResult).all()

        tag_counts = Counter()
        tag_confidence_sum = defaultdict(float)
        tag_confidence_count = defaultdict(int)
        creators_per_tag = defaultdict(set)

        for r in results:
            tag_counts[r.tag_name] += 1
            tag_confidence_sum[r.tag_name] += r.confidence
            tag_confidence_count[r.tag_name] += 1
            creators_per_tag[r.tag_name].add(r.creator_id)

        distribution = {}
        for tag, count in tag_counts.items():
            distribution[tag] = {
                "count": count,
                "unique_creators": len(creators_per_tag[tag]),
                "avg_confidence": tag_confidence_sum[tag] / tag_confidence_count[tag],
            }

        logger.info(f"Analyzed {len(distribution)} tags across {len(results)} tagging results")
        return distribution
    finally:
        session.close()


def task_detect_l2_gaps(**context):
    """Detect L1 categories where creators have no matching L2 tags."""
    from src.storage.models import Creator, TaggingResult
    from src.storage.postgres_client import get_session, init_db

    init_db()
    session = get_session()
    try:
        # Get taxonomy
        from src.graph.neo4j_client import get_neo4j_client
        from src.graph.taxonomy_loader import get_taxonomy_tree, init_taxonomy_graph
        from src.config import settings

        neo4j = get_neo4j_client()
        init_taxonomy_graph(neo4j, settings.taxonomy_path)
        taxonomy = get_taxonomy_tree(neo4j)

        l1_names = set(taxonomy.keys())
        l2_to_l1 = {}
        for l1, data in taxonomy.items():
            for l2 in data.get("children", {}):
                l2_to_l1[l2] = l1

        # Check each creator: does their L1 have at least one L2?
        creators = session.query(Creator).all()
        gaps = []

        for creator in creators:
            tags = session.query(TaggingResult).filter_by(creator_id=creator.id).all()
            creator_l1s = {t.tag_name for t in tags if t.tag_level == "L1"}
            creator_l2s = {t.tag_name for t in tags if t.tag_level == "L2"}

            l2_parents = {l2_to_l1.get(l2) for l2 in creator_l2s}

            for l1 in creator_l1s:
                if l1 not in l2_parents:
                    gaps.append({
                        "creator_id": creator.channel_id,
                        "creator_name": creator.name,
                        "l1_without_l2": l1,
                    })

        logger.info(f"Found {len(gaps)} L1→L2 gaps across {len(creators)} creators")
        return gaps
    finally:
        session.close()


def task_aggregate_suggestions(**context):
    """Aggregate LLM-suggested new tags and rank by frequency."""
    from src.storage.models import TagSuggestionLog
    from src.storage.postgres_client import get_session, init_db

    init_db()
    session = get_session()
    try:
        suggestions = (
            session.query(TagSuggestionLog)
            .filter_by(status="pending")
            .all()
        )

        tag_freq = Counter()
        tag_details = defaultdict(list)

        for s in suggestions:
            key = f"{s.parent_l1}:{s.suggested_tag}"
            tag_freq[key] += 1
            tag_details[key].append({
                "creator_id": s.creator_id,
                "reason": s.reason,
            })

        ranked = []
        for key, count in tag_freq.most_common(20):
            parent_l1, suggested_tag = key.split(":", 1)
            ranked.append({
                "suggested_tag": suggested_tag,
                "parent_l1": parent_l1,
                "frequency": count,
                "sample_reasons": [d["reason"] for d in tag_details[key][:3]],
            })

        logger.info(f"Top suggestions: {[r['suggested_tag'] for r in ranked[:5]]}")
        return ranked
    finally:
        session.close()


def task_generate_report(**context):
    """Compile a taxonomy health report from all analyses."""
    distribution = context["ti"].xcom_pull(task_ids="tag_distribution")
    gaps = context["ti"].xcom_pull(task_ids="detect_l2_gaps")
    suggestions = context["ti"].xcom_pull(task_ids="aggregate_suggestions")

    report = {
        "generated_at": datetime.utcnow().isoformat(),
        "tag_distribution": {
            "total_tags_in_use": len(distribution),
            "too_broad": [
                {"tag": tag, **info}
                for tag, info in distribution.items()
                if info["unique_creators"] > 30
            ],
            "too_narrow": [
                {"tag": tag, **info}
                for tag, info in distribution.items()
                if info["unique_creators"] <= 2
            ],
            "low_confidence": [
                {"tag": tag, **info}
                for tag, info in distribution.items()
                if info["avg_confidence"] < 0.7
            ],
        },
        "l2_coverage_gaps": {
            "total_gaps": len(gaps),
            "gap_by_l1": {},
        },
        "suggested_new_tags": suggestions,
    }

    # Aggregate gaps by L1
    gap_counter = Counter(g["l1_without_l2"] for g in gaps)
    report["l2_coverage_gaps"]["gap_by_l1"] = dict(gap_counter.most_common())

    # Save report
    report_path = f"data/taxonomy_health_{datetime.utcnow().strftime('%Y%m%d')}.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    logger.info(f"Taxonomy health report saved to {report_path}")
    logger.info(f"  Tags too broad: {len(report['tag_distribution']['too_broad'])}")
    logger.info(f"  Tags too narrow: {len(report['tag_distribution']['too_narrow'])}")
    logger.info(f"  L2 gaps: {report['l2_coverage_gaps']['total_gaps']}")
    logger.info(f"  New tag suggestions: {len(suggestions)}")

    return report_path


with DAG(
    dag_id="taxonomy_health",
    default_args=default_args,
    description="Analyze taxonomy usage, detect gaps, surface new tag suggestions",
    schedule="0 5 * * 0",  # Weekly on Sunday at 5 AM
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["analytics", "taxonomy"],
) as dag:

    dist = PythonOperator(task_id="tag_distribution", python_callable=task_tag_distribution)
    gaps = PythonOperator(task_id="detect_l2_gaps", python_callable=task_detect_l2_gaps)
    sugg = PythonOperator(task_id="aggregate_suggestions", python_callable=task_aggregate_suggestions)
    report = PythonOperator(task_id="generate_report", python_callable=task_generate_report)

    [dist, gaps, sugg] >> report
