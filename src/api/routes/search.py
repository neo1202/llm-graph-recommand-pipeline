from fastapi import APIRouter, Depends, Query

from src.api.dependencies import get_neo4j
from src.graph.neo4j_client import Neo4jClient
from src.graph.queries import (
    find_similar_creators,
    get_graph_stats,
    search_creators_by_tags,
)

router = APIRouter(prefix="/api/v1")


@router.get("/search")
def search_creators(
    tags: str = Query(..., description="Comma-separated tag names"),
    min_results: int = Query(5, description="Min results before generalization"),
    limit: int = Query(20, description="Max results"),
    neo4j: Neo4jClient = Depends(get_neo4j),
):
    """
    Two-phase creator search:
    1. Exact tag intersection
    2. Hierarchical generalization if results < min_results
    """
    tag_list = [t.strip() for t in tags.split(",") if t.strip()]
    results = search_creators_by_tags(neo4j, tag_list, min_results, limit)
    return {
        "query_tags": tag_list,
        "total_results": len(results),
        "results": results,
    }


@router.get("/creator/{channel_id}/similar")
def get_similar(
    channel_id: str,
    limit: int = Query(10),
    neo4j: Neo4jClient = Depends(get_neo4j),
):
    """Find similar creators based on weighted tag overlap."""
    results = find_similar_creators(neo4j, channel_id, limit)
    return {"channel_id": channel_id, "similar": results}


@router.get("/stats")
def pipeline_stats(neo4j: Neo4jClient = Depends(get_neo4j)):
    """Overall graph and pipeline statistics."""
    return get_graph_stats(neo4j)
