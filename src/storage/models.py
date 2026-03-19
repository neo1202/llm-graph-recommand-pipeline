import datetime

from sqlalchemy import (
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    func,
)
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    pass


class Creator(Base):
    __tablename__ = "creators"

    id = Column(Integer, primary_key=True)
    channel_id = Column(String(64), unique=True, nullable=False, index=True)
    name = Column(String(256), nullable=False)
    description = Column(Text, default="")
    subscriber_count = Column(Integer, default=0)
    region = Column(String(16), default="Global")
    video_titles = Column(Text, default="[]")  # JSON array of recent video titles
    added_by = Column(String(128), default="system")
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    tagging_results = relationship("TaggingResult", back_populates="creator")
    review_items = relationship("ReviewQueue", back_populates="creator")


class TaggingResult(Base):
    __tablename__ = "tagging_results"

    id = Column(Integer, primary_key=True)
    creator_id = Column(Integer, ForeignKey("creators.id"), nullable=False, index=True)
    tag_name = Column(String(128), nullable=False)
    tag_level = Column(String(4), nullable=False)  # L1, L2, L3
    confidence = Column(Float, nullable=False)
    created_at = Column(DateTime, server_default=func.now())

    creator = relationship("Creator", back_populates="tagging_results")


class AuditLog(Base):
    __tablename__ = "audit_log"

    id = Column(Integer, primary_key=True)
    creator_id = Column(Integer, ForeignKey("creators.id"), nullable=True)
    action = Column(String(64), nullable=False)  # e.g., "tagged", "flagged", "reviewed"
    details = Column(Text, default="")  # JSON string
    created_at = Column(DateTime, server_default=func.now())


class TagSuggestionLog(Base):
    __tablename__ = "tag_suggestions"

    id = Column(Integer, primary_key=True)
    creator_id = Column(Integer, ForeignKey("creators.id"), nullable=True, index=True)
    suggested_tag = Column(String(128), nullable=False)
    parent_l1 = Column(String(128), nullable=False)
    reason = Column(Text, default="")
    status = Column(String(16), default="pending")  # pending, accepted, rejected
    created_at = Column(DateTime, server_default=func.now())


class ReviewQueue(Base):
    __tablename__ = "review_queue"

    id = Column(Integer, primary_key=True)
    creator_id = Column(Integer, ForeignKey("creators.id"), nullable=False, index=True)
    reason = Column(String(256), nullable=False)  # e.g., "low_confidence", "conflict", "orphan"
    details = Column(Text, default="")
    status = Column(String(16), default="pending")  # pending, reviewed, dismissed
    created_at = Column(DateTime, server_default=func.now())
    reviewed_by = Column(String(128), nullable=True)
    reviewed_at = Column(DateTime, nullable=True)

    creator = relationship("Creator", back_populates="review_items")
