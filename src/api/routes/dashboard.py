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

    # Step 5: Store in PostgreSQL (staging only — Neo4j write deferred until approval)
    db_creator = Creator(
        channel_id=profile["channel_id"],
        name=profile["name"],
        description=profile["description"],
        subscriber_count=profile["subscriber_count"],
        region=profile["region"],
        video_titles=json.dumps(profile["recent_video_titles"]),
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

    # Step 6: Always add to review queue
    has_issues = len(qa_report.issues) > 0
    review_details = {
        "issues": qa_report.issues if has_issues else [],
        "candidate_tags": [
            {"tag": t.tag, "confidence": t.confidence}
            for t in tagging_result.candidate_tags
        ],
    }
    db.add(ReviewQueue(
        creator_id=db_creator.id,
        reason="flagged" if has_issues else "auto_pass",
        details=json.dumps(review_details),
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
):
    """Get review queue with full creator info and tags from PostgreSQL."""
    # Always compute counts from unfiltered query (fixes count bug)
    base_query = (
        db.query(ReviewQueue)
        .filter(ReviewQueue.status == status)
    )
    total_flagged = base_query.filter(ReviewQueue.reason == "flagged").count()
    total_passed = base_query.filter(ReviewQueue.reason == "auto_pass").count()

    # Now apply filter for items
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
        pg_tags = (
            db.query(TaggingResult)
            .filter_by(creator_id=c.id)
            .order_by(TaggingResult.tag_level, TaggingResult.confidence.desc())
            .all()
        )

        # Parse details: supports both old format (list) and new format (dict)
        raw_details = json.loads(rq.details) if rq.details else {}
        if isinstance(raw_details, list):
            issues = raw_details
            candidate_tags = []
        else:
            issues = raw_details.get("issues", [])
            candidate_tags = raw_details.get("candidate_tags", [])

        video_titles = json.loads(c.video_titles) if c.video_titles else []

        results.append({
            "review_id": rq.id,
            "has_issues": rq.reason == "flagged",
            "creator": {
                "channel_id": c.channel_id,
                "name": c.name,
                "description": c.description or "",
                "subscriber_count": c.subscriber_count,
                "region": c.region,
                "added_by": c.added_by or "system",
                "video_titles": video_titles,
            },
            "tags": [
                {"tag": t.tag_name, "level": t.tag_level, "confidence": t.confidence}
                for t in pg_tags
            ],
            "candidate_tags": candidate_tags,
            "issues": issues if isinstance(issues, list) else [],
            "status": rq.status,
            "created_at": rq.created_at.isoformat() if rq.created_at else None,
        })

    return {
        "items": results,
        "counts": {
            "total": total_flagged + total_passed,
            "flagged": total_flagged,
            "passed": total_passed,
        },
    }


@router.post("/review/{review_id}")
def resolve_review(
    review_id: int,
    action: str = Query(..., description="approve or dismiss"),
    reviewed_by: str = Query("user"),
    new_tags: str = Query(None, description="JSON array of tag objects [{tag, level, confidence}]"),
    db: Session = Depends(get_db),
    neo4j: Neo4jClient = Depends(get_neo4j),
):
    """Approve or dismiss a review item. On approve, write to Neo4j."""
    item = db.query(ReviewQueue).filter_by(id=review_id).first()
    if not item:
        return {"error": "Review item not found"}

    item.status = "reviewed" if action == "approve" else "dismissed"
    item.reviewed_by = reviewed_by
    item.reviewed_at = datetime.utcnow()

    creator = db.query(Creator).filter_by(id=item.creator_id).first()

    # If new tags provided, update PostgreSQL tagging results
    if new_tags:
        tag_list = json.loads(new_tags)
        db.query(TaggingResult).filter_by(creator_id=creator.id).delete()
        for t in tag_list:
            db.add(TaggingResult(
                creator_id=creator.id, tag_name=t["tag"],
                tag_level=t["level"], confidence=t.get("confidence", 1.0),
            ))

    # On approve: write creator + final tags to Neo4j (production graph)
    if action == "approve":
        final_tags = (
            db.query(TaggingResult).filter_by(creator_id=creator.id).all()
        )
        upsert_creator(neo4j, creator.channel_id, creator.name,
                       creator.region, added_by=creator.added_by or "system")
        clear_creator_tags(neo4j, creator.channel_id)
        for t in final_tags:
            add_creator_tag(neo4j, creator.channel_id, t.tag_name,
                            t.confidence, 6)

    db.add(AuditLog(
        creator_id=item.creator_id,
        action="reviewed",
        details=json.dumps({
            "review_id": review_id,
            "action": action,
            "reviewed_by": reviewed_by,
            "tags_modified": new_tags is not None,
        }),
    ))
    db.commit()

    return {"review_id": review_id, "status": item.status, "reviewed_by": reviewed_by}


@router.get("/taxonomy")
def get_taxonomy(neo4j: Neo4jClient = Depends(get_neo4j)):
    """Get full taxonomy tree for tag picker UI."""
    rows = neo4j.run_query("""
        MATCH (l1:Tag {level: 'L1'})
        OPTIONAL MATCH (l2:Tag {level: 'L2'})-[:CHILD_OF]->(l1)
        RETURN l1.name AS l1, collect(l2.name) AS children
        ORDER BY l1.name
    """)
    return [{"l1": r["l1"], "children": [c for c in r["children"] if c]} for r in rows]


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
