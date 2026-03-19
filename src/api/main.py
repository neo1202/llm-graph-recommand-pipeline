from contextlib import asynccontextmanager

from fastapi import FastAPI

from src.api.routes import health, search, tagging
from src.graph.neo4j_client import get_neo4j_client
from src.graph.taxonomy_loader import init_taxonomy_graph
from src.config import settings
from src.storage.postgres_client import init_db


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: initialize databases and taxonomy
    init_db()
    neo4j = get_neo4j_client()
    if neo4j.verify_connectivity():
        init_taxonomy_graph(neo4j, settings.taxonomy_path)
    yield
    # Shutdown
    neo4j.close()


app = FastAPI(
    title="iKala Creator Knowledge Graph API",
    description="Automated creator tagging and knowledge graph search platform",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(health.router)
app.include_router(search.router)
app.include_router(tagging.router)
