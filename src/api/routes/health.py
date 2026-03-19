from fastapi import APIRouter

from src.graph.neo4j_client import get_neo4j_client

router = APIRouter()


@router.get("/health")
def health_check():
    neo4j = get_neo4j_client()
    neo4j_ok = neo4j.verify_connectivity()
    return {
        "status": "healthy" if neo4j_ok else "degraded",
        "neo4j": "connected" if neo4j_ok else "disconnected",
    }
