"""BigQuery dataset and table setup for analytics."""

import logging

from src.config import settings

logger = logging.getLogger(__name__)

# Schema definitions for BigQuery tables
TABLES = {
    "review_metrics": [
        {"name": "date", "type": "DATE"},
        {"name": "total_reviewed", "type": "INTEGER"},
        {"name": "approved", "type": "INTEGER"},
        {"name": "dismissed", "type": "INTEGER"},
        {"name": "pending", "type": "INTEGER"},
        {"name": "approval_rate", "type": "FLOAT"},
        {"name": "tag_edit_rate", "type": "FLOAT"},
    ],
    "tag_distribution": [
        {"name": "date", "type": "DATE"},
        {"name": "tag_name", "type": "STRING"},
        {"name": "tag_level", "type": "STRING"},
        {"name": "usage_count", "type": "INTEGER"},
        {"name": "unique_creators", "type": "INTEGER"},
        {"name": "creator_share", "type": "FLOAT"},
        {"name": "avg_confidence", "type": "FLOAT"},
    ],
    "pipeline_stats": [
        {"name": "date", "type": "DATE"},
        {"name": "total_creators", "type": "INTEGER"},
        {"name": "total_tagged", "type": "INTEGER"},
        {"name": "total_errors", "type": "INTEGER"},
        {"name": "success_rate", "type": "FLOAT"},
        {"name": "flagged_count", "type": "INTEGER"},
        {"name": "auto_pass_count", "type": "INTEGER"},
        {"name": "flag_rate", "type": "FLOAT"},
        {"name": "estimated_cost_usd", "type": "FLOAT"},
    ],
}


def setup_bigquery():
    """Create BigQuery dataset and tables if they don't exist."""
    if not settings.gcp_project_id:
        logger.warning("GCP_PROJECT_ID not set — skipping BigQuery setup")
        return

    from google.cloud import bigquery

    client = bigquery.Client(project=settings.gcp_project_id)
    dataset_id = f"{settings.gcp_project_id}.{settings.bigquery_dataset}"

    # Create dataset
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"
    try:
        client.create_dataset(dataset, exists_ok=True)
        logger.info(f"Dataset {dataset_id} ready")
    except Exception as e:
        logger.error(f"Failed to create dataset: {e}")
        return

    # Create tables
    for table_name, schema_fields in TABLES.items():
        table_id = f"{dataset_id}.{table_name}"
        schema = [
            bigquery.SchemaField(f["name"], f["type"]) for f in schema_fields
        ]
        table = bigquery.Table(table_id, schema=schema)
        try:
            client.create_table(table, exists_ok=True)
            logger.info(f"Table {table_id} ready")
        except Exception as e:
            logger.error(f"Failed to create table {table_id}: {e}")

    logger.info("BigQuery setup complete")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    setup_bigquery()
