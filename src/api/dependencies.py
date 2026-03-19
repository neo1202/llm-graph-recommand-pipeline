"""FastAPI dependencies: database connections, shared resources."""

import json
from functools import lru_cache

from src.config import settings
from src.graph.neo4j_client import Neo4jClient, get_neo4j_client
from src.graph.taxonomy_loader import get_taxonomy_tree
from src.quality.gate import QualityGate
from src.storage.postgres_client import SessionLocal, init_db
from src.tagging.llm_tagger import LLMTagger


def get_db():
    """Yield a SQLAlchemy session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_neo4j() -> Neo4jClient:
    return get_neo4j_client()


@lru_cache()
def get_tagger() -> LLMTagger:
    neo4j = get_neo4j_client()
    taxonomy_tree = get_taxonomy_tree(neo4j)
    few_shots = []
    try:
        with open(settings.few_shot_path) as f:
            few_shots = json.load(f)
    except FileNotFoundError:
        pass
    return LLMTagger(taxonomy_tree, few_shots)


@lru_cache()
def get_quality_gate() -> QualityGate:
    neo4j = get_neo4j_client()
    taxonomy_tree = get_taxonomy_tree(neo4j)
    return QualityGate(taxonomy_tree)
