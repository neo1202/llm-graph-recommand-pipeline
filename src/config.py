from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    openai_api_key: str = ""
    openai_model: str = "gpt-4o-mini"

    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = "password"

    database_url: str = "postgresql://ikala:password@localhost:5432/ikala"

    youtube_api_key: str = ""

    taxonomy_path: str = "config/taxonomy.yaml"
    few_shot_path: str = "data/few_shot_examples.json"

    confidence_threshold: float = 0.7
    batch_size: int = 10

    model_config = {"env_file": ".env"}


settings = Settings()
