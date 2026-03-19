from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from src.config import settings
from src.storage.models import Base

engine = create_engine(settings.database_url, echo=False)
SessionLocal = sessionmaker(bind=engine)


def init_db():
    Base.metadata.create_all(bind=engine)
    # Add video_titles column if missing (no Alembic in this project)
    try:
        with engine.begin() as conn:
            conn.execute(text(
                "ALTER TABLE creators ADD COLUMN IF NOT EXISTS video_titles TEXT DEFAULT '[]'"
            ))
    except Exception:
        pass


def get_session() -> Session:
    return SessionLocal()
