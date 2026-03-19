"""Dashboard API routes: review queue, add creator, graph visualization."""

import json
import re
from datetime import datetime

import httpx
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from src.api.dependencies import get_db, get_neo4j, get_quality_gate, get_tagger
from src.config import settings
from src.graph.neo4j_client import Neo4jClient
from src.graph.queries import add_creator_tag, clear_creator_tags, upsert_creator
from src.quality.gate import QualityGate
from src.storage.models import AuditLog, Creator, ReviewQueue, TaggingResult
from src.tagging.llm_tagger import LLMTagger
from src.tagging.schema import CreatorInput

router = APIRouter(prefix="/api/v1")

YOUTUBE_API_BASE = "https://www.googleapis.com/youtube/v3"


def _resolve_channel_id(query: str) -> str | None:
    """Resolve a YouTube URL or channel name to a channel ID."""
    # Already a channel ID
    if query.startswith("UC") and len(query) == 24:
        return query

    # Extract from URL patterns
    url_patterns = [
        r"youtube\.com/channel/(UC[\w-]{22})",
        r"youtube\.com/@([\w.-]+)",
        r"youtube\.com/c/([\w.-]+)",
    ]
    for pattern in url_patterns:
        match = re.search(pattern, query)
        if match:
            value = match.group(1)
            if value.startswith("UC"):
                return value
            # Handle @username or /c/name — search API
            return _search_channel(value)

    # Plain text: search by name
    return _search_channel(query)


def _search_channel(query: str) -> str | None:
    """Search YouTube for a channel by name."""
    api_key = settings.youtube_api_key
    if not api_key:
        return None
    try:
        resp = httpx.get(
            f"{YOUTUBE_API_BASE}/search",
            params={
                "key": api_key,
                "q": query,
                "type": "channel",
                "maxResults": 1,
                "part": "snippet",
            },
            timeout=10,
        )
        items = resp.json().get("items", [])
        return items[0]["snippet"]["channelId"] if items else None
    except Exception:
        return None


def _fetch_channel_profile(channel_id: str) -> dict | None:
    """Fetch full creator profile from YouTube API."""
    api_key = settings.youtube_api_key
    if not api_key:
        return None
    try:
        client = httpx.Client(timeout=15)
        # Channel details
        resp = client.get(f"{YOUTUBE_API_BASE}/channels", params={
            "key": api_key, "id": channel_id,
            "part": "snippet,statistics,contentDetails",
        })
        items = resp.json().get("items", [])
        if not items:
            return None

        item = items[0]
        snippet = item.get("snippet", {})
        stats = item.get("statistics", {})
        uploads = (
            item.get("contentDetails", {})
            .get("relatedPlaylists", {})
            .get("uploads", "")
        )

        # Recent videos
        titles = []
        if uploads:
            vresp = client.get(f"{YOUTUBE_API_BASE}/playlistItems", params={
                "key": api_key, "playlistId": uploads,
                "part": "snippet", "maxResults": 10,
            })
            titles = [v["snippet"]["title"] for v in vresp.json().get("items", [])]

        client.close()
        return {
            "channel_id": channel_id,
            "name": snippet.get("title", ""),
            "description": snippet.get("description", ""),
            "subscriber_count": int(stats.get("subscriberCount", 0)),
            "region": snippet.get("country", "TW"),
            "recent_video_titles": titles,
        }
    except Exception:
        return None


@router.get("/creators/preview")
def preview_creator(
    query: str = Query(..., description="YouTube URL, channel name, or channel ID"),
    db: Session = Depends(get_db),
):
    """Preview a creator's YouTube profile before adding to the graph."""
    channel_id = _resolve_channel_id(query)
    if not channel_id:
        return {"error": f"Could not find YouTube channel for: {query}"}

    # Check if already exists
    existing = db.query(Creator).filter_by(channel_id=channel_id).first()
    if existing:
        return {"error": "Creator already exists in the database", "name": existing.name, "channel_id": channel_id}

    profile = _fetch_channel_profile(channel_id)
    if not profile:
        return {"error": f"Could not fetch channel data for {channel_id}"}

    return profile


@router.post("/creators/add")
def add_creator_by_query(
    query: str = Query(..., description="YouTube URL, channel name, or channel ID"),
    added_by: str = Query("user", description="Who is adding this creator"),
    db: Session = Depends(get_db),
    neo4j: Neo4jClient = Depends(get_neo4j),
    tagger: LLMTagger = Depends(get_tagger),
    quality_gate: QualityGate = Depends(get_quality_gate),
):
    """Add a creator by YouTube URL or name, auto-fetch and tag."""
    # Step 1: Resolve to channel ID
    channel_id = _resolve_channel_id(query)
    if not channel_id:
        return {"error": f"Could not find YouTube channel for: {query}"}

    # Step 2: Check if already exists
    existing = db.query(Creator).filter_by(channel_id=channel_id).first()
    if existing:
        return {"error": "Creator already exists", "name": existing.name, "channel_id": channel_id}

    # Step 3: Fetch profile from YouTube
    profile = _fetch_channel_profile(channel_id)
    if not profile:
        return {"error": f"Could not fetch channel data for {channel_id}"}

    # Step 4: Run tagging pipeline
    creator_input = CreatorInput(
        channel_id=profile["channel_id"],
        name=profile["name"],
        description=profile["description"],
        recent_video_titles=profile["recent_video_titles"],
        subscriber_count=profile["subscriber_count"],
        region=profile["region"],
    )
    tagging_result = tagger.tag_creator(creator_input)
    qa_report = quality_gate.validate(tagging_result)

    # Step 5: Store in PostgreSQL
    db_creator = Creator(
        channel_id=profile["channel_id"],
        name=profile["name"],
        description=profile["description"],
        subscriber_count=profile["subscriber_count"],
        region=profile["region"],
        added_by=added_by,
    )
    db.add(db_creator)
    db.flush()

    for tag in qa_report.filtered_l1_tags + qa_report.filtered_l2_tags:
        level = "L1" if tag in qa_report.filtered_l1_tags else "L2"
        db.add(TaggingResult(
            creator_id=db_creator.id, tag_name=tag.tag,
            tag_level=level, confidence=tag.confidence,
        ))

    # Step 6: Store in Neo4j
    upsert_creator(neo4j, profile["channel_id"], profile["name"],
                   profile["region"], added_by=added_by)
    clear_creator_tags(neo4j, profile["channel_id"])
    for tag in qa_report.filtered_l1_tags + qa_report.filtered_l2_tags:
        add_creator_tag(neo4j, profile["channel_id"], tag.tag,
                        tag.confidence, tagger.prompt_version)

    # Step 7: Always add to review queue
    has_issues = len(qa_report.issues) > 0
    db.add(ReviewQueue(
        creator_id=db_creator.id,
        reason="flagged" if has_issues else "auto_pass",
        details=json.dumps(qa_report.issues) if has_issues else "[]",
    ))

    db.add(AuditLog(
        creator_id=db_creator.id, action="added",
        details=json.dumps({"added_by": added_by, "source": query}),
    ))
    db.commit()

    return {
        "channel_id": profile["channel_id"],
        "name": profile["name"],
        "subscriber_count": profile["subscriber_count"],
        "added_by": added_by,
        "l1_tags": [{"tag": t.tag, "confidence": t.confidence} for t in qa_report.filtered_l1_tags],
        "l2_tags": [{"tag": t.tag, "confidence": t.confidence} for t in qa_report.filtered_l2_tags],
        "issues": qa_report.issues,
    }


@router.get("/review")
def get_review_queue(
    status: str = Query("pending", description="Filter by status"),
    filter_type: str = Query("all", description="all, flagged, or passed"),
    db: Session = Depends(get_db),
    neo4j: Neo4jClient = Depends(get_neo4j),
):
    """Get review queue with full creator info and tags."""
    query = (
        db.query(ReviewQueue, Creator)
        .join(Creator, ReviewQueue.creator_id == Creator.id)
        .filter(ReviewQueue.status == status)
    )
    if filter_type == "flagged":
        query = query.filter(ReviewQueue.reason == "flagged")
    elif filter_type == "passed":
        query = query.filter(ReviewQueue.reason == "auto_pass")

    items = query.order_by(ReviewQueue.created_at.desc()).all()

    results = []
    for rq, c in items:
        # Fetch tags from Neo4j
        tags = neo4j.run_query(
            """
            MATCH (cr:Creator {channel_id: $cid})-[r:HAS_TAG]->(t:Tag)
            RETURN t.name AS tag, t.level AS level, r.confidence AS confidence
            ORDER BY t.level, r.confidence DESC
            """,
            {"cid": c.channel_id},
        )

        issues = json.loads(rq.details) if rq.details else []

        results.append({
            "review_id": rq.id,
            "has_issues": rq.reason == "flagged",
            "creator": {
                "channel_id": c.channel_id,
                "name": c.name,
                "description": (c.description or "")[:300],
                "subscriber_count": c.subscriber_count,
                "region": c.region,
                "added_by": c.added_by or "system",
            },
            "tags": [
                {"tag": t["tag"], "level": t["level"], "confidence": t["confidence"]}
                for t in tags
            ],
            "issues": issues if isinstance(issues, list) else [],
            "status": rq.status,
            "created_at": rq.created_at.isoformat() if rq.created_at else None,
        })

    return {
        "items": results,
        "counts": {
            "total": len(results),
            "flagged": sum(1 for r in results if r["has_issues"]),
            "passed": sum(1 for r in results if not r["has_issues"]),
        },
    }


@router.post("/review/{review_id}")
def resolve_review(
    review_id: int,
    action: str = Query(..., description="approve or dismiss"),
    reviewed_by: str = Query("user"),
    new_tags: str = Query(None, description="Comma-separated tag names to replace current tags"),
    db: Session = Depends(get_db),
    neo4j: Neo4jClient = Depends(get_neo4j),
):
    """Approve or dismiss a review item. Optionally override tags."""
    item = db.query(ReviewQueue).filter_by(id=review_id).first()
    if not item:
        return {"error": "Review item not found"}

    item.status = "reviewed" if action == "approve" else "dismissed"
    item.reviewed_by = reviewed_by
    item.reviewed_at = datetime.utcnow()

    creator = db.query(Creator).filter_by(id=item.creator_id).first()

    # If new tags provided, update Neo4j and PostgreSQL
    if new_tags:
        tag_list = [t.strip() for t in new_tags.split(",") if t.strip()]
        clear_creator_tags(neo4j, creator.channel_id)

        # Delete old tagging results
        db.query(TaggingResult).filter_by(creator_id=creator.id).delete()

        for tag_name in tag_list:
            # Determine level from taxonomy
            level = "L1" if tag_name in _get_l1_names(neo4j) else "L2"
            add_creator_tag(neo4j, creator.channel_id, tag_name, 1.0, 6)
            db.add(TaggingResult(
                creator_id=creator.id, tag_name=tag_name,
                tag_level=level, confidence=1.0,
            ))

    db.add(AuditLog(
        creator_id=item.creator_id,
        action="reviewed",
        details=json.dumps({
            "review_id": review_id,
            "action": action,
            "reviewed_by": reviewed_by,
            "new_tags": new_tags,
        }),
    ))
    db.commit()

    return {"review_id": review_id, "status": item.status, "reviewed_by": reviewed_by}


def _get_l1_names(neo4j: Neo4jClient) -> set:
    """Cache-friendly helper to get L1 tag names."""
    results = neo4j.run_query("MATCH (t:Tag {level: 'L1'}) RETURN t.name AS name")
    return {r["name"] for r in results}

    return {"review_id": review_id, "status": item.status, "reviewed_by": reviewed_by}


@router.get("/graph/data")
def get_graph_visualization(
    neo4j: Neo4jClient = Depends(get_neo4j),
    limit: int = Query(50, description="Max creators to include"),
):
    """Get graph data for vis.js visualization."""
    # Get taxonomy
    taxonomy = neo4j.run_query("""
        MATCH (l1:Tag {level: 'L1'})
        OPTIONAL MATCH (l2:Tag {level: 'L2'})-[:CHILD_OF]->(l1)
        RETURN l1.name AS l1, collect(l2.name) AS children
    """)

    # Get creators and their tags
    creators = neo4j.run_query("""
        MATCH (c:Creator)-[r:HAS_TAG]->(t:Tag)
        WITH c, collect({tag: t.name, level: t.level, confidence: r.confidence}) AS tags
        RETURN c.channel_id AS id, c.name AS name, c.region AS region,
               c.added_by AS added_by, tags
        ORDER BY size(tags) DESC
        LIMIT $limit
    """, {"limit": limit})

    # Build vis.js nodes and edges
    nodes = []
    edges = []
    node_ids = set()

    # L1 tag nodes
    for row in taxonomy:
        l1 = row["l1"]
        nodes.append({"id": l1, "label": l1, "group": "L1",
                       "color": "#e74c3c", "shape": "diamond", "size": 25})
        node_ids.add(l1)
        for l2 in row["children"]:
            if l2:
                nodes.append({"id": l2, "label": l2, "group": "L2",
                               "color": "#3498db", "shape": "dot", "size": 15})
                edges.append({"from": l2, "to": l1, "label": "CHILD_OF",
                               "color": "#bdc3c7", "dashes": True})
                node_ids.add(l2)

    # Creator nodes
    for c in creators:
        nodes.append({
            "id": c["id"], "label": c["name"], "group": "Creator",
            "color": "#2ecc71", "shape": "box", "size": 10,
            "title": f"{c['name']} [{c['region']}]\nAdded by: {c.get('added_by', 'system')}",
        })
        for tag in c["tags"]:
            if tag["tag"] in node_ids:
                edges.append({
                    "from": c["id"], "to": tag["tag"],
                    "label": f"{tag['confidence']:.1f}",
                    "color": "#27ae60",
                })

    return {"nodes": nodes, "edges": edges}
