"""
Discover and fetch top Taiwan YouTuber profiles using YouTube Data API v3.
Searches across multiple category keywords, deduplicates, and saves profiles.
"""

import json
import logging
import time

import httpx

from src.config import settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

YOUTUBE_API_BASE = "https://www.googleapis.com/youtube/v3"

# Search terms per category to find diverse TW creators
SEARCH_QUERIES = {
    "Technology": ["科技 開箱", "手機評測", "電腦組裝", "3C"],
    "Entertainment": ["遊戲實況", "搞笑", "音樂 cover", "動畫"],
    "Lifestyle": ["台灣 vlog", "旅遊", "日常生活", "極簡生活"],
    "Food": ["台灣美食", "料理教學", "夜市", "吃播"],
    "Education": ["教學", "科普", "學英文", "程式教學"],
    "Finance": ["投資理財", "股票分析", "被動收入"],
    "Beauty_Fashion": ["美妝教學", "穿搭", "保養"],
    "Sports_Fitness": ["健身", "運動訓練", "瑜珈"],
    "Kids_Family": ["親子", "育兒", "兒童教育"],
}


def search_channels(client: httpx.Client, api_key: str, query: str,
                     max_results: int = 15) -> list[str]:
    """Search YouTube for channels matching a query in Taiwan region."""
    try:
        resp = client.get(
            f"{YOUTUBE_API_BASE}/search",
            params={
                "key": api_key,
                "q": query,
                "type": "channel",
                "regionCode": "TW",
                "relevanceLanguage": "zh-Hant",
                "maxResults": max_results,
                "part": "snippet",
            },
        )
        resp.raise_for_status()
        items = resp.json().get("items", [])
        return [item["snippet"]["channelId"] for item in items]
    except Exception as e:
        logger.error(f"Search failed for '{query}': {e}")
        return []


def fetch_channel_details(client: httpx.Client, api_key: str,
                           channel_ids: list[str]) -> list[dict]:
    """Fetch channel details in batch (max 50 per request)."""
    results = []
    for i in range(0, len(channel_ids), 50):
        batch = channel_ids[i:i + 50]
        try:
            resp = client.get(
                f"{YOUTUBE_API_BASE}/channels",
                params={
                    "key": api_key,
                    "id": ",".join(batch),
                    "part": "snippet,statistics,contentDetails",
                },
            )
            resp.raise_for_status()
            results.extend(resp.json().get("items", []))
        except Exception as e:
            logger.error(f"Channel fetch failed: {e}")
        time.sleep(0.2)
    return results


def fetch_recent_videos(client: httpx.Client, api_key: str,
                         uploads_playlist_id: str, max_results: int = 10) -> list[str]:
    """Fetch recent video titles from a channel's uploads playlist."""
    try:
        resp = client.get(
            f"{YOUTUBE_API_BASE}/playlistItems",
            params={
                "key": api_key,
                "playlistId": uploads_playlist_id,
                "part": "snippet",
                "maxResults": max_results,
            },
        )
        resp.raise_for_status()
        return [item["snippet"]["title"] for item in resp.json().get("items", [])]
    except Exception as e:
        logger.error(f"Video fetch failed for playlist {uploads_playlist_id}: {e}")
        return []


def discover_tw_creators(target_count: int = 100) -> list[dict]:
    """Discover Taiwan YouTubers by searching across categories."""
    api_key = settings.youtube_api_key
    if not api_key:
        raise ValueError("YOUTUBE_API_KEY not set in .env")

    client = httpx.Client(timeout=30)
    seen_channel_ids = set()
    discovered = []  # (channel_id, source_category)

    # Phase 1: Search across all category keywords
    for category, queries in SEARCH_QUERIES.items():
        for query in queries:
            if len(seen_channel_ids) >= target_count * 2:
                break

            logger.info(f"Searching: '{query}' ({category})")
            channel_ids = search_channels(client, api_key, query)

            for cid in channel_ids:
                if cid not in seen_channel_ids:
                    seen_channel_ids.add(cid)
                    discovered.append((cid, category))

            time.sleep(0.3)

        if len(seen_channel_ids) >= target_count * 2:
            break

    logger.info(f"Discovered {len(discovered)} unique channels")

    # Phase 2: Fetch channel details (batch)
    all_channel_ids = [cid for cid, _ in discovered]
    channel_details = fetch_channel_details(client, api_key, all_channel_ids)

    # Build lookup
    detail_map = {}
    for item in channel_details:
        cid = item["id"]
        snippet = item.get("snippet", {})
        stats = item.get("statistics", {})
        uploads = (
            item.get("contentDetails", {})
            .get("relatedPlaylists", {})
            .get("uploads", "")
        )
        detail_map[cid] = {
            "channel_id": cid,
            "name": snippet.get("title", ""),
            "description": snippet.get("description", ""),
            "subscriber_count": int(stats.get("subscriberCount", 0)),
            "video_count": int(stats.get("videoCount", 0)),
            "country": snippet.get("country", ""),
            "uploads_playlist_id": uploads,
        }

    # Phase 3: Sort by subscriber count and pick top N
    ranked = sorted(
        [(cid, cat) for cid, cat in discovered if cid in detail_map],
        key=lambda x: detail_map[x[0]]["subscriber_count"],
        reverse=True,
    )[:target_count]

    # Phase 4: Fetch recent videos for selected creators
    creators = []
    for i, (cid, source_category) in enumerate(ranked):
        info = detail_map[cid]
        uploads_id = info.pop("uploads_playlist_id")
        video_count = info.pop("video_count")

        logger.info(
            f"[{i+1}/{len(ranked)}] Fetching videos: {info['name']} "
            f"({info['subscriber_count']:,} subs)"
        )

        video_titles = []
        if uploads_id:
            video_titles = fetch_recent_videos(client, api_key, uploads_id)
            time.sleep(0.2)

        info["recent_video_titles"] = video_titles
        info["region"] = "TW"
        info["source_category"] = source_category
        creators.append(info)

    client.close()
    return creators


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Fetch Taiwan YouTuber profiles")
    parser.add_argument("--count", type=int, default=100, help="Number of creators to fetch")
    parser.add_argument("--output", default="data/tw_creators.json", help="Output path")
    args = parser.parse_args()

    creators = discover_tw_creators(target_count=args.count)

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(creators, f, indent=2, ensure_ascii=False)

    logger.info(f"Saved {len(creators)} creators to {args.output}")

    # Print summary
    print(f"\n{'='*60}")
    print(f"Fetched {len(creators)} Taiwan YouTubers")
    print(f"{'='*60}")
    for i, c in enumerate(creators):
        print(f"  {i+1:3d}. {c['name']:<30s} {c['subscriber_count']:>12,} subs  "
              f"({len(c['recent_video_titles'])} videos)")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
