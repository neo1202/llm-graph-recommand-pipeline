from src.graph.neo4j_client import Neo4jClient


def upsert_creator(client: Neo4jClient, channel_id: str, name: str, region: str = "Global"):
    """Create or update a Creator node in Neo4j."""
    client.write_query(
        """
        MERGE (c:Creator {channel_id: $channel_id})
        SET c.name = $name, c.region = $region
        """,
        {"channel_id": channel_id, "name": name, "region": region},
    )


def add_creator_tag(
    client: Neo4jClient,
    channel_id: str,
    tag_name: str,
    confidence: float,
    version: int,
):
    """Create HAS_TAG relationship between Creator and Tag."""
    client.write_query(
        """
        MATCH (c:Creator {channel_id: $channel_id})
        MATCH (t:Tag {name: $tag_name})
        MERGE (c)-[r:HAS_TAG]->(t)
        SET r.confidence = $confidence, r.version = $version, r.updated_at = datetime()
        """,
        {
            "channel_id": channel_id,
            "tag_name": tag_name,
            "confidence": confidence,
            "version": version,
        },
    )


def clear_creator_tags(client: Neo4jClient, channel_id: str):
    """Remove all existing tags for a creator (before re-tagging)."""
    client.write_query(
        """
        MATCH (c:Creator {channel_id: $channel_id})-[r:HAS_TAG]->()
        DELETE r
        """,
        {"channel_id": channel_id},
    )


def search_creators_by_tags(
    client: Neo4jClient,
    tags: list[str],
    min_results: int = 5,
    limit: int = 20,
) -> list[dict]:
    """
    Two-phase search:
    1. Exact intersection — creators matching ALL requested tags
    2. If results < min_results, hierarchical generalization via CHILD_OF
    """
    # Phase 1: Exact match
    exact_results = client.run_query(
        """
        WITH $tags AS required_tags
        MATCH (c:Creator)-[r:HAS_TAG]->(t:Tag)
        WHERE t.name IN required_tags
        WITH c, count(DISTINCT t) AS matched, avg(r.confidence) AS avg_conf,
             collect(t.name) AS matched_tags
        WHERE matched = size($tags)
        RETURN c.channel_id AS channel_id, c.name AS name, c.region AS region,
               matched_tags, avg_conf, 'exact' AS match_type
        ORDER BY avg_conf DESC
        LIMIT $limit
        """,
        {"tags": tags, "limit": limit},
    )

    if len(exact_results) >= min_results:
        return exact_results

    # Phase 2: Hierarchical generalization
    generalized_results = client.run_query(
        """
        WITH $tags AS required_tags
        UNWIND required_tags AS tag_name
        MATCH (t:Tag {name: tag_name})
        OPTIONAL MATCH (t)-[:CHILD_OF]->(parent:Tag)
        OPTIONAL MATCH (sibling:Tag)-[:CHILD_OF]->(parent)
        WITH collect(DISTINCT t.name) + collect(DISTINCT sibling.name) AS expanded_tags
        MATCH (c:Creator)-[r:HAS_TAG]->(t:Tag)
        WHERE t.name IN expanded_tags
        WITH c, count(DISTINCT t) AS matched, avg(r.confidence) AS avg_conf,
             collect(t.name) AS matched_tags
        RETURN c.channel_id AS channel_id, c.name AS name, c.region AS region,
               matched_tags, avg_conf, 'generalized' AS match_type
        ORDER BY matched DESC, avg_conf DESC
        LIMIT $limit
        """,
        {"tags": tags, "limit": limit},
    )

    # Deduplicate: exact results first, then generalized
    seen = {r["channel_id"] for r in exact_results}
    combined = list(exact_results)
    for r in generalized_results:
        if r["channel_id"] not in seen:
            combined.append(r)
            seen.add(r["channel_id"])

    return combined[:limit]


def find_similar_creators(
    client: Neo4jClient,
    channel_id: str,
    limit: int = 10,
) -> list[dict]:
    """
    Find similar creators using weighted tag overlap.
    Rare tags (fewer creators) get higher weight.
    """
    return client.run_query(
        """
        MATCH (source:Creator {channel_id: $channel_id})-[:HAS_TAG]->(t:Tag)
        WITH source, collect(t) AS source_tags
        UNWIND source_tags AS tag
        MATCH (other:Creator)-[r:HAS_TAG]->(tag)
        WHERE other <> source
        WITH other, tag,
             1.0 / (size([(c:Creator)-[:HAS_TAG]->(tag) | c]) + 1) AS tag_rarity,
             r.confidence AS conf
        WITH other,
             sum(tag_rarity * conf) AS similarity_score,
             collect(tag.name) AS shared_tags
        RETURN other.channel_id AS channel_id, other.name AS name,
               shared_tags, round(similarity_score * 100) / 100 AS score
        ORDER BY score DESC
        LIMIT $limit
        """,
        {"channel_id": channel_id, "limit": limit},
    )


def get_orphan_creators(client: Neo4jClient) -> list[dict]:
    """Find creators with no tags (orphan nodes)."""
    return client.run_query(
        """
        MATCH (c:Creator)
        WHERE NOT (c)-[:HAS_TAG]->()
        RETURN c.channel_id AS channel_id, c.name AS name
        """
    )


def get_graph_stats(client: Neo4jClient) -> dict:
    """Get overall graph statistics."""
    results = client.run_query(
        """
        MATCH (c:Creator) WITH count(c) AS creators
        MATCH (t:Tag) WITH creators, count(t) AS tags
        MATCH ()-[r:HAS_TAG]->() WITH creators, tags, count(r) AS tag_edges
        RETURN creators, tags, tag_edges
        """
    )
    return results[0] if results else {"creators": 0, "tags": 0, "tag_edges": 0}
