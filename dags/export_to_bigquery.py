"""
DAG: export_to_bigquery
Runs daily — exports key metrics from PostgreSQL to BigQuery for analytics.

BigQuery tables:
- bq_pipeline_runs: daily pipeline run statistics
- bq_tag_distribution: tag usage counts and confidence
- bq_review_metrics: human approval rate, tag edit rate
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
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}


def _get_bq_client():
    """Get BigQuery client, or None if GCP not configured."""
    from src.config import settings
    if not settings.gcp_project_id:
        logger.warning("GCP_PROJECT_ID not set — BigQuery export will be skipped (dry run)")
        return None
    from google.cloud import bigquery
    return bigquery.Client(project=settings.gcp_project_id)


def task_export_review_metrics(**context):
    """Calculate and export human approval rate and tag edit rate."""
    from src.config import settings
    from src.storage.models import AuditLog, ReviewQueue
    from src.storage.postgres_client import get_session, init_db

    init_db()
    session = get_session()
    try:
        # Reviews completed today
        today = datetime.utcnow().date()
        reviewed = (
            session.query(ReviewQueue)
            .filter(ReviewQueue.status.in_(["reviewed", "dismissed"]))
            .all()
        )

        total_reviewed = len(reviewed)
        approved = sum(1 for r in reviewed if r.status == "reviewed")
        dismissed = sum(1 for r in reviewed if r.status == "dismissed")
        pending = session.query(ReviewQueue).filter_by(status="pending").count()

        # Tag edit rate: check audit log for reviews with tags_modified=true
        review_logs = (
            session.query(AuditLog)
            .filter_by(action="reviewed")
            .all()
        )
        edited = 0
        for log in review_logs:
            try:
                details = json.loads(log.details)
                if details.get("tags_modified"):
                    edited += 1
            except (json.JSONDecodeError, TypeError):
                pass

        metrics = {
            "date": today.isoformat(),
            "total_reviewed": total_reviewed,
            "approved": approved,
            "dismissed": dismissed,
            "pending": pending,
            "approval_rate": approved / max(total_reviewed, 1),
            "tag_edit_rate": edited / max(total_reviewed, 1),
        }

        logger.info(f"Review metrics: approval_rate={metrics['approval_rate']:.2%}, "
                     f"tag_edit_rate={metrics['tag_edit_rate']:.2%}")

        # Export to BigQuery
        bq = _get_bq_client()
        if bq:
            table_id = f"{settings.gcp_project_id}.{settings.bigquery_dataset}.review_metrics"
            bq.insert_rows_json(table_id, [metrics])
            logger.info(f"Exported to {table_id}")

        return metrics
    finally:
        session.close()


def task_export_tag_distribution(**context):
    """Export tag usage distribution to BigQuery."""
    from src.config import settings
    from src.storage.models import Creator, TaggingResult
    from src.storage.postgres_client import get_session, init_db

    init_db()
    session = get_session()
    try:
        results = session.query(TaggingResult).all()
        total_creators = session.query(Creator).count()

        tag_data = defaultdict(lambda: {"count": 0, "creators": set(), "conf_sum": 0.0})
        for r in results:
            tag_data[r.tag_name]["count"] += 1
            tag_data[r.tag_name]["creators"].add(r.creator_id)
            tag_data[r.tag_name]["conf_sum"] += r.confidence
            tag_data[r.tag_name]["level"] = r.tag_level

        rows = []
        today = datetime.utcnow().date().isoformat()
        for tag, d in tag_data.items():
            rows.append({
                "date": today,
                "tag_name": tag,
                "tag_level": d["level"],
                "usage_count": d["count"],
                "unique_creators": len(d["creators"]),
                "creator_share": len(d["creators"]) / max(total_creators, 1),
                "avg_confidence": d["conf_sum"] / d["count"],
            })

        logger.info(f"Tag distribution: {len(rows)} tags across {total_creators} creators")

        bq = _get_bq_client()
        if bq:
            table_id = f"{settings.gcp_project_id}.{settings.bigquery_dataset}.tag_distribution"
            bq.insert_rows_json(table_id, rows)
            logger.info(f"Exported {len(rows)} rows to {table_id}")

        return {"tags": len(rows), "creators": total_creators}
    finally:
        session.close()


def task_export_pipeline_stats(**context):
    """Export pipeline run statistics (cost per creator, success rate)."""
    from src.config import settings
    from src.storage.models import AuditLog, Creator, ReviewQueue
    from src.storage.postgres_client import get_session, init_db

    init_db()
    session = get_session()
    try:
        total_creators = session.query(Creator).count()
        total_tagged = (
            session.query(AuditLog)
            .filter(AuditLog.action.in_(["tagged", "added"]))
            .count()
        )
        total_errors = session.query(AuditLog).filter_by(action="error").count()
        flagged = session.query(ReviewQueue).filter_by(reason="flagged").count()
        auto_pass = session.query(ReviewQueue).filter_by(reason="auto_pass").count()

        stats = {
            "date": datetime.utcnow().date().isoformat(),
            "total_creators": total_creators,
            "total_tagged": total_tagged,
            "total_errors": total_errors,
            "success_rate": total_tagged / max(total_tagged + total_errors, 1),
            "flagged_count": flagged,
            "auto_pass_count": auto_pass,
            "flag_rate": flagged / max(flagged + auto_pass, 1),
            # Estimated cost: ~$0.002 per creator (2 GPT-4o-mini calls)
            "estimated_cost_usd": total_tagged * 0.002,
        }

        logger.info(f"Pipeline stats: {total_creators} creators, "
                     f"success_rate={stats['success_rate']:.2%}, "
                     f"cost=${stats['estimated_cost_usd']:.2f}")

        bq = _get_bq_client()
        if bq:
            table_id = f"{settings.gcp_project_id}.{settings.bigquery_dataset}.pipeline_stats"
            bq.insert_rows_json(table_id, [stats])
            logger.info(f"Exported to {table_id}")

        return stats
    finally:
        session.close()


with DAG(
    dag_id="export_to_bigquery",
    default_args=default_args,
    description="Export pipeline metrics to BigQuery",
    schedule="0 6 * * *",  # Daily at 6 AM
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["analytics", "bigquery"],
) as dag:

    review = PythonOperator(task_id="export_review_metrics", python_callable=task_export_review_metrics)
    tags = PythonOperator(task_id="export_tag_distribution", python_callable=task_export_tag_distribution)
    stats = PythonOperator(task_id="export_pipeline_stats", python_callable=task_export_pipeline_stats)

    # All three run in parallel — no dependencies between them
    [review, tags, stats]
