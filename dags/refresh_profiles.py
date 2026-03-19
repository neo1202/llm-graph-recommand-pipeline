"""
DAG: refresh_profiles
Runs weekly — scans PostgreSQL for creators not updated in 30+ days,
re-fetches their YouTube profile (subscriber count, recent videos).
"""

import json
import logging
import time
from datetime import datetime, timedelta

import httpx
from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

YOUTUBE_API_BASE = "https://www.googleapis.com/youtube/v3"
STALE_DAYS = 30

default_args = {
    "owner": "ikala-data",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def task_find_stale(**context):
    """Find creators whose profiles haven't been updated in 30+ days."""
    from src.storage.models import Creator
    from src.storage.postgres_client import get_session, init_db

    init_db()
    session = get_session()
    try:
        cutoff = datetime.utcnow() - timedelta(days=STALE_DAYS)
        stale = (
            session.query(Creator)
            .filter(Creator.updated_at < cutoff)
            .order_by(Creator.updated_at.asc())
            .all()
        )
        ids = [c.channel_id for c in stale]
        logger.info(f"Found {len(ids)} stale creators (not updated in {STALE_DAYS}+ days)")
        return ids
    finally:
        session.close()


def task_refresh(**context):
    """Re-fetch YouTube profiles and update PostgreSQL."""
    from src.config import settings
    from src.storage.models import AuditLog, Creator
    from src.storage.postgres_client import get_session, init_db

    init_db()
    stale_ids = context["ti"].xcom_pull(task_ids="find_stale")
    if not stale_ids:
        logger.info("No stale profiles to refresh")
        return 0

    api_key = settings.youtube_api_key
    if not api_key:
        raise ValueError("YOUTUBE_API_KEY not set")

    client = httpx.Client(timeout=30)
    session = get_session()
    updated = 0

    try:
        for i in range(0, len(stale_ids), 50):
            batch = stale_ids[i:i + 50]
            resp = client.get(f"{YOUTUBE_API_BASE}/channels", params={
                "key": api_key, "id": ",".join(batch),
                "part": "snippet,statistics,contentDetails",
            })
            resp.raise_for_status()

            for item in resp.json().get("items", []):
                cid = item["id"]
                creator = session.query(Creator).filter_by(channel_id=cid).first()
                if not creator:
                    continue

                snippet = item.get("snippet", {})
                stats = item.get("statistics", {})
                uploads = (
                    item.get("contentDetails", {})
                    .get("relatedPlaylists", {})
                    .get("uploads", "")
                )

                old_subs = creator.subscriber_count
                creator.subscriber_count = int(stats.get("subscriberCount", 0))
                creator.description = snippet.get("description", creator.description)
                creator.updated_at = datetime.utcnow()

                # Refresh recent videos
                if uploads:
                    try:
                        vresp = client.get(f"{YOUTUBE_API_BASE}/playlistItems", params={
                            "key": api_key, "playlistId": uploads,
                            "part": "snippet", "maxResults": 10,
                        })
                        titles = [v["snippet"]["title"] for v in vresp.json().get("items", [])]
                        creator.video_titles = json.dumps(titles)
                    except Exception:
                        pass
                    time.sleep(0.2)

                session.add(AuditLog(
                    creator_id=creator.id,
                    action="profile_refreshed",
                    details=json.dumps({
                        "old_subs": old_subs,
                        "new_subs": creator.subscriber_count,
                        "dag": "refresh_profiles",
                    }),
                ))
                updated += 1

            time.sleep(0.3)

        session.commit()
    except Exception as e:
        session.rollback()
        logger.error(f"Refresh failed: {e}")
        raise
    finally:
        client.close()
        session.close()

    logger.info(f"Refreshed {updated} creator profiles")
    return updated


with DAG(
    dag_id="refresh_profiles",
    default_args=default_args,
    description="Refresh stale creator profiles from YouTube API",
    schedule="0 4 * * 3",  # Weekly on Wednesday at 4 AM
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["maintenance", "youtube"],
) as dag:

    find = PythonOperator(task_id="find_stale", python_callable=task_find_stale)
    refresh = PythonOperator(task_id="refresh_profiles", python_callable=task_refresh)

    find >> refresh
