import yaml

from src.graph.neo4j_client import Neo4jClient


def load_taxonomy(yaml_path: str) -> dict:
    with open(yaml_path) as f:
        return yaml.safe_load(f)


def init_taxonomy_graph(client: Neo4jClient, yaml_path: str):
    """Load taxonomy YAML into Neo4j as a hierarchical tag graph."""
    taxonomy = load_taxonomy(yaml_path)

    # Clear existing taxonomy (idempotent reload)
    client.write_query("MATCH (t:Tag) DETACH DELETE t")

    # Create constraints
    client.write_query(
        "CREATE CONSTRAINT tag_name_unique IF NOT EXISTS "
        "FOR (t:Tag) REQUIRE t.name IS UNIQUE"
    )

    categories = taxonomy.get("categories", {})
    version = taxonomy.get("version", 1)

    for l1_name, l1_data in categories.items():
        # Create L1 node
        client.write_query(
            """
            CREATE (t:Tag {
                name: $name,
                level: 'L1',
                description: $description,
                version: $version
            })
            """,
            {
                "name": l1_name,
                "description": l1_data.get("description", ""),
                "version": version,
            },
        )

        # Create L2 children
        for l2_name, l2_data in l1_data.get("children", {}).items():
            client.write_query(
                """
                CREATE (t:Tag {
                    name: $name,
                    level: 'L2',
                    description: $description,
                    version: $version
                })
                """,
                {
                    "name": l2_name,
                    "description": l2_data.get("description", ""),
                    "version": version,
                },
            )

            # Create CHILD_OF relationship (L2 -> L1)
            client.write_query(
                """
                MATCH (child:Tag {name: $child_name})
                MATCH (parent:Tag {name: $parent_name})
                CREATE (child)-[:CHILD_OF]->(parent)
                """,
                {"child_name": l2_name, "parent_name": l1_name},
            )

    # Load conflict rules
    conflict_rules = taxonomy.get("conflict_rules", [])
    for pair in conflict_rules:
        if len(pair) == 2:
            client.write_query(
                """
                MATCH (a:Tag {name: $tag_a})
                MATCH (b:Tag {name: $tag_b})
                CREATE (a)-[:CONFLICTS_WITH]->(b)
                CREATE (b)-[:CONFLICTS_WITH]->(a)
                """,
                {"tag_a": pair[0], "tag_b": pair[1]},
            )

    return {
        "l1_count": len(categories),
        "l2_count": sum(
            len(v.get("children", {})) for v in categories.values()
        ),
        "conflict_rules": len(conflict_rules),
        "version": version,
    }


def get_taxonomy_tree(client: Neo4jClient) -> dict:
    """Retrieve the full taxonomy tree from Neo4j for prompt construction."""
    records = client.run_query(
        """
        MATCH (l2:Tag {level: 'L2'})-[:CHILD_OF]->(l1:Tag {level: 'L1'})
        RETURN l1.name AS l1, l1.description AS l1_desc,
               collect({name: l2.name, description: l2.description}) AS children
        ORDER BY l1.name
        """
    )
    tree = {}
    for record in records:
        tree[record["l1"]] = {
            "description": record["l1_desc"],
            "children": {
                c["name"]: c["description"] for c in record["children"]
            },
        }
    return tree


def get_l2_tags_for_l1(client: Neo4jClient, l1_name: str) -> list[dict]:
    """Get all L2 tags under a specific L1 category."""
    return client.run_query(
        """
        MATCH (l2:Tag {level: 'L2'})-[:CHILD_OF]->(l1:Tag {name: $l1_name})
        RETURN l2.name AS name, l2.description AS description
        """,
        {"l1_name": l1_name},
    )
