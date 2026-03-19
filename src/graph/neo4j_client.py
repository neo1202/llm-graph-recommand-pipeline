from contextlib import contextmanager

from neo4j import GraphDatabase

from src.config import settings


class Neo4jClient:
    def __init__(self, uri: str = None, user: str = None, password: str = None):
        self._driver = GraphDatabase.driver(
            uri or settings.neo4j_uri,
            auth=(user or settings.neo4j_user, password or settings.neo4j_password),
        )

    def close(self):
        self._driver.close()

    @contextmanager
    def session(self):
        session = self._driver.session()
        try:
            yield session
        finally:
            session.close()

    def run_query(self, query: str, parameters: dict = None) -> list[dict]:
        with self.session() as session:
            result = session.run(query, parameters or {})
            return [record.data() for record in result]

    def write_query(self, query: str, parameters: dict = None):
        with self.session() as session:
            session.run(query, parameters or {})

    def verify_connectivity(self) -> bool:
        try:
            self._driver.verify_connectivity()
            return True
        except Exception:
            return False


_client: Neo4jClient | None = None


def get_neo4j_client() -> Neo4jClient:
    global _client
    if _client is None:
        _client = Neo4jClient()
    return _client
