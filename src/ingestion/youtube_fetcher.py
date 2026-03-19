"""YouTube Data API v3 client for fetching creator channel data."""

import logging
import time

import httpx

logger = logging.getLogger(__name__)

YOUTUBE_API_BASE = "https://www.googleapis.com/youtube/v3"


class YouTubeFetcher:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.client = httpx.Client(timeout=30)
        self._request_count = 0

    def _rate_limit(self):
        """Simple rate limiter: 1 request per 100ms."""
        self._request_count += 1
        if self._request_count % 10 == 0:
            time.sleep(1)

    def fetch_channel(self, channel_id: str) -> dict | None:
        """Fetch channel snippet and statistics."""
        self._rate_limit()
        try:
            resp = self.client.get(
                f"{YOUTUBE_API_BASE}/channels",
                params={
                    "key": self.api_key,
                    "id": channel_id,
                    "part": "snippet,statistics,contentDetails",
                },
            )
            resp.raise_for_status()
            items = resp.json().get("items", [])
            if not items:
                return None

            item = items[0]
            snippet = item.get("snippet", {})
            stats = item.get("statistics", {})

            return {
                "channel_id": channel_id,
                "name": snippet.get("title", ""),
                "description": snippet.get("description", ""),
                "subscriber_count": int(stats.get("subscriberCount", 0)),
                "video_count": int(stats.get("videoCount", 0)),
                "country": snippet.get("country", ""),
            }
        except Exception as e:
            logger.error(f"Failed to fetch channel {channel_id}: {e}")
            return None

    def fetch_recent_videos(self, channel_id: str, max_results: int = 10) -> list[str]:
        """Fetch recent video titles for a channel."""
        self._rate_limit()
        try:
            # First get uploads playlist ID
            resp = self.client.get(
                f"{YOUTUBE_API_BASE}/channels",
                params={
                    "key": self.api_key,
                    "id": channel_id,
                    "part": "contentDetails",
                },
            )
            resp.raise_for_status()
            items = resp.json().get("items", [])
            if not items:
                return []

            uploads_id = (
                items[0]
                .get("contentDetails", {})
                .get("relatedPlaylists", {})
                .get("uploads", "")
            )
            if not uploads_id:
                return []

            # Fetch recent videos from uploads playlist
            self._rate_limit()
            resp = self.client.get(
                f"{YOUTUBE_API_BASE}/playlistItems",
                params={
                    "key": self.api_key,
                    "playlistId": uploads_id,
                    "part": "snippet",
                    "maxResults": max_results,
                },
            )
            resp.raise_for_status()

            return [
                item["snippet"]["title"]
                for item in resp.json().get("items", [])
            ]
        except Exception as e:
            logger.error(f"Failed to fetch videos for {channel_id}: {e}")
            return []

    def fetch_creator_profile(self, channel_id: str) -> dict | None:
        """Fetch complete creator profile (channel info + recent videos)."""
        channel = self.fetch_channel(channel_id)
        if not channel:
            return None

        videos = self.fetch_recent_videos(channel_id)
        channel["recent_video_titles"] = videos
        return channel
