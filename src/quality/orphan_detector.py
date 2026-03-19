"""Detect orphan nodes in the knowledge graph."""

from src.graph.neo4j_client import Neo4jClient


def find_orphan_creators(client: Neo4jClient) -> list[dict]:
    """Find creators that have no tags assigned."""
    return client.run_query(
        """
        MATCH (c:Creator)
        WHERE NOT (c)-[:HAS_TAG]->()
        RETURN c.channel_id AS channel_id, c.name AS name
        ORDER BY c.name
        """
    )


def find_unused_tags(client: Neo4jClient) -> list[dict]:
    """Find L2 tags that no creator is using — may indicate irrelevant tags."""
    return client.run_query(
        """
        MATCH (t:Tag {level: 'L2'})
        WHERE NOT ()-[:HAS_TAG]->(t)
        RETURN t.name AS tag_name, t.description AS description
        ORDER BY t.name
        """
    )


def find_unlinked_tags(client: Neo4jClient) -> list[dict]:
    """Find tags that aren't properly linked in the hierarchy."""
    return client.run_query(
        """
        MATCH (t:Tag {level: 'L2'})
        WHERE NOT (t)-[:CHILD_OF]->()
        RETURN t.name AS tag_name, 'L2 tag without parent' AS issue
        UNION
        MATCH (t:Tag {level: 'L1'})
        WHERE NOT ()-[:CHILD_OF]->(t)
        RETURN t.name AS tag_name, 'L1 tag with no children' AS issue
        """
    )
