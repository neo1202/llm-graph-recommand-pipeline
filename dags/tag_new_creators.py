"""
DAG: tag_new_creators
Runs daily — picks up un-tagged creators from PostgreSQL and runs the LLM tagging pipeline.
Stores results in PostgreSQL staging. Neo4j write is deferred until review approval.
"""

import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

default_args = {
    "owner": "ikala-data",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}


def _init_services():
    """Initialize databases and load taxonomy."""
    from src.graph.neo4j_client import get_neo4j_client
    from src.graph.taxonomy_loader import get_taxonomy_tree, init_taxonomy_graph
    from src.config import settings
    from src.storage.postgres_client import init_db

    init_db()
    neo4j = get_neo4j_client()
    init_taxonomy_graph(neo4j, settings.taxonomy_path)
    return get_taxonomy_tree(neo4j)


def task_find_untagged(**context):
    """Find creators in PostgreSQL that have no tagging results yet."""
    from sqlalchemy import func
    from src.storage.models import Creator, TaggingResult
    from src.storage.postgres_client import get_session

    session = get_session()
    try:
        tagged_ids = session.query(TaggingResult.creator_id).distinct().subquery()
        untagged = (
            session.query(Creator)
            .filter(~Creator.id.in_(session.query(tagged_ids.c.creator_id)))
            .all()
        )
        creator_ids = [c.channel_id for c in untagged]
        logger.info(f"Found {len(creator_ids)} untagged creators")
        return creator_ids
    finally:
        session.close()


def task_tag_creators(**context):
    """Run LLM tagging pipeline on untagged creators."""
    import json
    from src.config import settings
    from src.graph.neo4j_client import get_neo4j_client
    from src.graph.taxonomy_loader import get_taxonomy_tree, init_taxonomy_graph
    from src.quality.gate import QualityGate
    from src.storage.models import AuditLog, Creator, ReviewQueue, TaggingResult, TagSuggestionLog
    from src.storage.postgres_client import get_session, init_db
    from src.tagging.llm_tagger import LLMTagger
    from src.tagging.schema import CreatorInput

    creator_ids = context["ti"].xcom_pull(task_ids="find_untagged")
    if not creator_ids:
        logger.info("No untagged creators to process")
        return {"processed": 0, "flagged": 0, "passed": 0, "failed": 0}

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

    stats = {"processed": 0, "flagged": 0, "passed": 0, "failed": 0}

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

                tagging_result = tagger.tag_creator(creator_input)
                qa_report = quality_gate.validate(tagging_result)

                for tag in qa_report.filtered_l1_tags + qa_report.filtered_l2_tags:
                    level = "L1" if tag in qa_report.filtered_l1_tags else "L2"
                    session.add(TaggingResult(
                        creator_id=creator.id, tag_name=tag.tag,
                        tag_level=level, confidence=tag.confidence,
                    ))

                # Store LLM suggested new tags
                for suggestion in tagging_result.suggested_new_tags:
                    session.add(TagSuggestionLog(
                        creator_id=creator.id,
                        suggested_tag=suggestion.suggested_tag,
                        parent_l1=suggestion.parent_l1,
                        reason=suggestion.reason,
                    ))

                has_issues = len(qa_report.issues) > 0
                session.add(ReviewQueue(
                    creator_id=creator.id,
                    reason="flagged" if has_issues else "auto_pass",
                    details=json.dumps(qa_report.issues) if has_issues else "[]",
                ))
                session.add(AuditLog(
                    creator_id=creator.id, action="tagged",
                    details=json.dumps({
                        "l1": [t.tag for t in qa_report.filtered_l1_tags],
                        "l2": [t.tag for t in qa_report.filtered_l2_tags],
                        "dag": "tag_new_creators",
                    }),
                ))

                stats["processed"] += 1
                if has_issues:
                    stats["flagged"] += 1
                else:
                    stats["passed"] += 1

                if stats["processed"] % 10 == 0:
                    session.commit()

            except Exception as e:
                stats["failed"] += 1
                logger.error(f"Failed to tag {creator.name}: {e}")

        session.commit()
    finally:
        session.close()

    logger.info(f"Tagging complete: {stats}")
    return stats


with DAG(
    dag_id="tag_new_creators",
    default_args=default_args,
    description="Run LLM tagging pipeline on untagged creators",
    schedule="0 2 * * *",  # Daily at 2 AM
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["pipeline", "tagging"],
) as dag:

    find = PythonOperator(task_id="find_untagged", python_callable=task_find_untagged)
    tag = PythonOperator(task_id="tag_creators", python_callable=task_tag_creators)

    find >> tag
