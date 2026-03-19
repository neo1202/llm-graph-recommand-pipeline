"""Initialize Neo4j with taxonomy from YAML config."""

from src.config import settings
from src.graph.neo4j_client import get_neo4j_client
from src.graph.taxonomy_loader import init_taxonomy_graph, get_taxonomy_tree


def main():
    client = get_neo4j_client()

    if not client.verify_connectivity():
        print("ERROR: Cannot connect to Neo4j. Is it running?")
        print("  Try: docker-compose up -d neo4j")
        return

    print(f"Loading taxonomy from {settings.taxonomy_path}...")
    stats = init_taxonomy_graph(client, settings.taxonomy_path)
    print(f"Loaded: {stats}")

    tree = get_taxonomy_tree(client)
    print(f"\nTaxonomy tree ({len(tree)} L1 categories):")
    for l1, data in sorted(tree.items()):
        children = list(data["children"].keys())
        print(f"  {l1} ({len(children)} children): {', '.join(children[:5])}...")

    client.close()


if __name__ == "__main__":
    main()
