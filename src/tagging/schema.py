from pydantic import BaseModel


class TagPrediction(BaseModel):
    tag: str
    confidence: float


class L1Result(BaseModel):
    categories: list[TagPrediction]


class L2Result(BaseModel):
    tags: list[TagPrediction]
    candidate_tags: list[TagPrediction] = []


class TagSuggestion(BaseModel):
    suggested_tag: str
    parent_l1: str
    reason: str


class TaggingOutput(BaseModel):
    creator_id: str
    l1_tags: list[TagPrediction]
    l2_tags: list[TagPrediction]
    candidate_tags: list[TagPrediction] = []
    model: str
    prompt_version: int
    suggested_new_tags: list[TagSuggestion] = []


class CreatorInput(BaseModel):
    channel_id: str
    name: str
    description: str
    recent_video_titles: list[str]
    subscriber_count: int = 0
    region: str = "Global"
