"""
DAG: discover_creators
Runs weekly — searches YouTube API for new creators across category keywords.
Deduplicates against existing PostgreSQL records and stores new discoveries.
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

SEARCH_QUERIES = {
    "Technology": ["科技 開箱", "手機評測", "3C"],
    "Entertainment": ["遊戲實況", "搞笑", "音樂 cover"],
    "Lifestyle": ["台灣 vlog", "旅遊", "日常生活"],
    "Food": ["台灣美食", "料理教學", "吃播"],
    "Education": ["教學", "科普", "程式教學"],
    "Finance": ["投資理財", "股票分析"],
    "Beauty_Fashion": ["美妝教學", "穿搭"],
    "Sports_Fitness": ["健身", "運動訓練"],
    "Kids_Family": ["親子", "育兒"],
}

default_args = {
    "owner": "ikala-data",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def task_search_youtube(**context):
    """Search YouTube for channels across all category keywords."""
    from src.config import settings

    api_key = settings.youtube_api_key
    if not api_key:
        raise ValueError("YOUTUBE_API_KEY not set")

    client = httpx.Client(timeout=30)
    discovered = {}  # channel_id → source_category

    for category, queries in SEARCH_QUERIES.items():
        for query in queries:
            try:
                resp = client.get(f"{YOUTUBE_API_BASE}/search", params={
                    "key": api_key, "q": query, "type": "channel",
                    "regionCode": "TW", "relevanceLanguage": "zh-Hant",
                    "maxResults": 15, "part": "snippet",
                })
                resp.raise_for_status()
                for item in resp.json().get("items", []):
                    cid = item["snippet"]["channelId"]
                    if cid not in discovered:
                        discovered[cid] = category
            except Exception as e:
                logger.warning(f"Search failed for '{query}': {e}")
            time.sleep(0.3)

    client.close()
    logger.info(f"YouTube search found {len(discovered)} unique channels")
    return list(discovered.keys())


def task_filter_new(**context):
    """Filter out channels that already exist in PostgreSQL."""
    from src.storage.models import Creator
    from src.storage.postgres_client import get_session, init_db

    init_db()
    all_channel_ids = context["ti"].xcom_pull(task_ids="search_youtube")
    if not all_channel_ids:
        return []

    session = get_session()
    try:
        existing = {
            r.channel_id
            for r in session.query(Creator.channel_id).all()
        }
        new_ids = [cid for cid in all_channel_ids if cid not in existing]
        logger.info(f"New channels: {len(new_ids)} (filtered {len(existing)} existing)")
        return new_ids
    finally:
        session.close()


def task_fetch_and_store(**context):
    """Fetch YouTube profiles for new channels and store in PostgreSQL."""
    import json
    from src.config import settings
    from src.storage.models import AuditLog, Creator
    from src.storage.postgres_client import get_session, init_db

    init_db()
    new_ids = context["ti"].xcom_pull(task_ids="filter_new")
    if not new_ids:
        logger.info("No new creators to fetch")
        return 0

    api_key = settings.youtube_api_key
    client = httpx.Client(timeout=30)
    session = get_session()
    count = 0

    try:
        # Batch fetch channel details
        for i in range(0, len(new_ids), 50):
            batch = new_ids[i:i + 50]
            resp = client.get(f"{YOUTUBE_API_BASE}/channels", params={
                "key": api_key, "id": ",".join(batch),
                "part": "snippet,statistics,contentDetails",
            })
            resp.raise_for_status()

            for item in resp.json().get("items", []):
                cid = item["id"]
                snippet = item.get("snippet", {})
                stats = item.get("statistics", {})
                uploads = (
                    item.get("contentDetails", {})
                    .get("relatedPlaylists", {})
                    .get("uploads", "")
                )

                # Fetch recent videos
                titles = []
                if uploads:
                    try:
                        vresp = client.get(f"{YOUTUBE_API_BASE}/playlistItems", params={
                            "key": api_key, "playlistId": uploads,
                            "part": "snippet", "maxResults": 10,
                        })
                        titles = [v["snippet"]["title"] for v in vresp.json().get("items", [])]
                    except Exception:
                        pass
                    time.sleep(0.2)

                creator = Creator(
                    channel_id=cid,
                    name=snippet.get("title", ""),
                    description=snippet.get("description", ""),
                    subscriber_count=int(stats.get("subscriberCount", 0)),
                    region=snippet.get("country", "TW"),
                    video_titles=json.dumps(titles),
                    added_by="airflow:discover",
                )
                session.add(creator)
                session.add(AuditLog(
                    action="discovered",
                    details=json.dumps({"channel_id": cid, "dag": "discover_creators"}),
                ))
                count += 1

            time.sleep(0.3)

        session.commit()
    except Exception as e:
        session.rollback()
        logger.error(f"Fetch failed: {e}")
        raise
    finally:
        client.close()
        session.close()

    logger.info(f"Stored {count} new creators")
    return count


with DAG(
    dag_id="discover_creators",
    default_args=default_args,
    description="Discover new YouTube creators via search API",
    schedule="0 6 * * 1",  # Weekly on Monday at 6 AM
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["ingestion", "youtube"],
) as dag:

    search = PythonOperator(task_id="search_youtube", python_callable=task_search_youtube)
    filter_new = PythonOperator(task_id="filter_new", python_callable=task_filter_new)
    fetch = PythonOperator(task_id="fetch_and_store", python_callable=task_fetch_and_store)

    search >> filter_new >> fetch
