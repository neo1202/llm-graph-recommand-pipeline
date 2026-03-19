"""
DAG: retag_outdated
Manual trigger only — re-tags all creators when taxonomy has a major version update.
Runs in background, clears old tags and re-runs the LLM pipeline with the new taxonomy.
"""

import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

default_args = {
    "owner": "ikala-data",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def task_list_all_creators(**context):
    """Get all creator channel IDs for re-tagging."""
    from src.storage.models import Creator
    from src.storage.postgres_client import get_session, init_db

    init_db()
    session = get_session()
    try:
        creators = session.query(Creator.channel_id).all()
        ids = [c.channel_id for c in creators]
        logger.info(f"Will re-tag {len(ids)} creators")
        return ids
    finally:
        session.close()


def task_retag_all(**context):
    """Clear existing tags and re-run LLM tagging with current taxonomy."""
    from src.config import settings
    from src.graph.neo4j_client import get_neo4j_client
    from src.graph.taxonomy_loader import get_taxonomy_tree, init_taxonomy_graph
    from src.quality.gate import QualityGate
    from src.storage.models import AuditLog, Creator, ReviewQueue, TaggingResult, TagSuggestionLog
    from src.storage.postgres_client import get_session, init_db
    from src.tagging.llm_tagger import LLMTagger
    from src.tagging.schema import CreatorInput

    creator_ids = context["ti"].xcom_pull(task_ids="list_all_creators")
    if not creator_ids:
        return {"processed": 0}

    init_db()
    neo4j = get_neo4j_client()
    init_taxonomy_graph(neo4j, settings.taxonomy_path)
    taxonomy_tree = get_taxonomy_tree(neo4j)

    few_shots = []
    try:
        with open(settings.few_shot_path) as f:
            few_shots = json.load(f)
    except FileNotFoundError:
        pass

    tagger = LLMTagger(taxonomy_tree, few_shots)
    quality_gate = QualityGate(taxonomy_tree)
    session = get_session()

    stats = {"processed": 0, "failed": 0}

    try:
        for channel_id in creator_ids:
            creator = session.query(Creator).filter_by(channel_id=channel_id).first()
            if not creator:
                continue

            try:
                video_titles = json.loads(creator.video_titles) if creator.video_titles else []
                creator_input = CreatorInput(
                    channel_id=creator.channel_id,
                    name=creator.name,
                    description=creator.description or "",
                    recent_video_titles=video_titles,
                    subscriber_count=creator.subscriber_count or 0,
                    region=creator.region or "Global",
                )

                # Clear old tags
                session.query(TaggingResult).filter_by(creator_id=creator.id).delete()

                # Re-tag
                tagging_result = tagger.tag_creator(creator_input)
                qa_report = quality_gate.validate(tagging_result)

                for tag in qa_report.filtered_l1_tags + qa_report.filtered_l2_tags:
                    level = "L1" if tag in qa_report.filtered_l1_tags else "L2"
                    session.add(TaggingResult(
                        creator_id=creator.id, tag_name=tag.tag,
                        tag_level=level, confidence=tag.confidence,
                    ))

                for suggestion in tagging_result.suggested_new_tags:
                    session.add(TagSuggestionLog(
                        creator_id=creator.id,
                        suggested_tag=suggestion.suggested_tag,
                        parent_l1=suggestion.parent_l1,
                        reason=suggestion.reason,
                    ))

                # Add to review queue for re-review
                has_issues = len(qa_report.issues) > 0
                session.add(ReviewQueue(
                    creator_id=creator.id,
                    reason="flagged" if has_issues else "auto_pass",
                    details=json.dumps(qa_report.issues) if has_issues else "[]",
                ))

                session.add(AuditLog(
                    creator_id=creator.id, action="retagged",
                    details=json.dumps({
                        "l1": [t.tag for t in qa_report.filtered_l1_tags],
                        "l2": [t.tag for t in qa_report.filtered_l2_tags],
                        "dag": "retag_outdated",
                        "prompt_version": tagger.prompt_version,
                    }),
                ))

                stats["processed"] += 1
                if stats["processed"] % 10 == 0:
                    session.commit()
                    logger.info(f"Re-tagged {stats['processed']}/{len(creator_ids)}")

            except Exception as e:
                stats["failed"] += 1
                logger.error(f"Failed to re-tag {creator.name}: {e}")

        session.commit()
    finally:
        session.close()

    logger.info(f"Re-tagging complete: {stats}")
    return stats


with DAG(
    dag_id="retag_outdated",
    default_args=default_args,
    description="Re-tag all creators after taxonomy update (manual trigger)",
    schedule=None,  # Manual trigger only
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["pipeline", "taxonomy", "manual"],
) as dag:

    list_creators = PythonOperator(task_id="list_all_creators", python_callable=task_list_all_creators)
    retag = PythonOperator(task_id="retag_all", python_callable=task_retag_all)

    list_creators >> retag
