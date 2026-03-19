import json

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from src.api.dependencies import get_db, get_neo4j, get_quality_gate, get_tagger
from src.graph.neo4j_client import Neo4jClient
from src.graph.queries import add_creator_tag, clear_creator_tags, upsert_creator
from src.quality.gate import QualityGate
from src.storage.models import AuditLog, Creator, ReviewQueue, TaggingResult
from src.tagging.llm_tagger import LLMTagger
from src.tagging.schema import CreatorInput

router = APIRouter(prefix="/api/v1")


@router.post("/tag")
def tag_creator(
    creator: CreatorInput,
    db: Session = Depends(get_db),
    neo4j: Neo4jClient = Depends(get_neo4j),
    tagger: LLMTagger = Depends(get_tagger),
    quality_gate: QualityGate = Depends(get_quality_gate),
):
    """Tag a single creator on-demand (same pipeline as batch)."""

    # Step 1: LLM Tagging
    tagging_result = tagger.tag_creator(creator)

    # Step 2: Quality Gates
    qa_report = quality_gate.validate(tagging_result)

    # Step 3: Store in PostgreSQL
    db_creator = db.query(Creator).filter_by(channel_id=creator.channel_id).first()
    if not db_creator:
        db_creator = Creator(
            channel_id=creator.channel_id,
            name=creator.name,
            description=creator.description,
            subscriber_count=creator.subscriber_count,
            region=creator.region,
        )
        db.add(db_creator)
        db.flush()

    for tag in qa_report.filtered_l1_tags + qa_report.filtered_l2_tags:
        level = "L1" if tag in qa_report.filtered_l1_tags else "L2"
        db.add(TaggingResult(
            creator_id=db_creator.id,
            tag_name=tag.tag,
            tag_level=level,
            confidence=tag.confidence,
            llm_model=tagging_result.model,
            prompt_version=tagging_result.prompt_version,
        ))

    # Step 4: Store in Neo4j
    upsert_creator(neo4j, creator.channel_id, creator.name, creator.region)
    clear_creator_tags(neo4j, creator.channel_id)
    for tag in qa_report.filtered_l1_tags + qa_report.filtered_l2_tags:
        add_creator_tag(
            neo4j, creator.channel_id, tag.tag,
            tag.confidence, tagging_result.prompt_version,
        )

    # Step 5: Flag issues
    if qa_report.issues:
        for issue in qa_report.issues:
            db.add(ReviewQueue(
                creator_id=db_creator.id,
                reason=issue["type"],
                details=json.dumps(issue),
            ))

    db.add(AuditLog(
        creator_id=db_creator.id,
        action="tagged",
        details=json.dumps({
            "l1": [t.tag for t in qa_report.filtered_l1_tags],
            "l2": [t.tag for t in qa_report.filtered_l2_tags],
            "issues": len(qa_report.issues),
        }),
    ))
    db.commit()

    return {
        "creator_id": creator.channel_id,
        "l1_tags": [{"tag": t.tag, "confidence": t.confidence} for t in qa_report.filtered_l1_tags],
        "l2_tags": [{"tag": t.tag, "confidence": t.confidence} for t in qa_report.filtered_l2_tags],
        "quality": {
            "passed": qa_report.passed,
            "issues": qa_report.issues,
        },
    }


@router.get("/creator/{channel_id}")
def get_creator(
    channel_id: str,
    neo4j: Neo4jClient = Depends(get_neo4j),
):
    """Get a creator's profile and tags from the knowledge graph."""
    results = neo4j.run_query(
        """
        MATCH (c:Creator {channel_id: $channel_id})
        OPTIONAL MATCH (c)-[r:HAS_TAG]->(t:Tag)
        RETURN c.name AS name, c.region AS region,
               collect({tag: t.name, level: t.level, confidence: r.confidence}) AS tags
        """,
        {"channel_id": channel_id},
    )
    if not results:
        return {"error": "Creator not found"}

    record = results[0]
    return {
        "channel_id": channel_id,
        "name": record["name"],
        "region": record["region"],
        "tags": record["tags"],
    }
