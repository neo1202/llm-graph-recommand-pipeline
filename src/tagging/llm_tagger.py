"""2-stage LLM tagging pipeline using OpenAI structured output."""

import json
import logging

from openai import OpenAI

from src.config import settings
from src.tagging.prompts import (
    STAGE1_PROMPT,
    STAGE2_PROMPT,
    SYSTEM_PROMPT,
    format_few_shot,
    format_taxonomy_l1,
    format_taxonomy_l2,
    format_video_titles,
)
from src.tagging.schema import CreatorInput, L1Result, L2Result, TaggingOutput

logger = logging.getLogger(__name__)


class LLMTagger:
    def __init__(
        self,
        taxonomy_tree: dict,
        few_shot_examples: list[dict] | None = None,
        prompt_version: int = 6,
    ):
        self.client = OpenAI(api_key=settings.openai_api_key)
        self.model = settings.openai_model
        self.taxonomy_tree = taxonomy_tree
        self.few_shot_examples = few_shot_examples or []
        self.prompt_version = prompt_version

    def _call_llm(self, prompt: str) -> dict:
        """Call OpenAI API with JSON mode."""
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
            response_format={"type": "json_object"},
            temperature=0.1,  # Low temperature for consistency
            max_tokens=512,
        )
        content = response.choices[0].message.content
        return json.loads(content)

    def stage1_classify(self, creator: CreatorInput) -> L1Result:
        """Stage 1: Predict L1 broad categories."""
        prompt = STAGE1_PROMPT.format(
            taxonomy_l1=format_taxonomy_l1(self.taxonomy_tree),
            name=creator.name,
            description=creator.description,
            video_titles=format_video_titles(creator.recent_video_titles),
            few_shot_section=format_few_shot(self.few_shot_examples),
        )
        raw = self._call_llm(prompt)
        return L1Result.model_validate(raw)

    def stage2_tag(self, creator: CreatorInput, l1_result: L1Result) -> L2Result:
        """Stage 2: Predict specific L2 tags within L1 categories."""
        l1_categories = [c.tag for c in l1_result.categories]
        prompt = STAGE2_PROMPT.format(
            l1_categories=", ".join(l1_categories),
            taxonomy_l2=format_taxonomy_l2(self.taxonomy_tree, l1_categories),
            name=creator.name,
            description=creator.description,
            video_titles=format_video_titles(creator.recent_video_titles),
        )
        raw = self._call_llm(prompt)
        return L2Result.model_validate(raw)

    def tag_creator(self, creator: CreatorInput) -> TaggingOutput:
        """Full 2-stage tagging pipeline for a single creator."""
        logger.info(f"Tagging creator: {creator.name} ({creator.channel_id})")

        # Stage 1: L1 classification
        l1_result = self.stage1_classify(creator)
        logger.info(
            f"  Stage 1 → {[c.tag for c in l1_result.categories]}"
        )

        # Stage 2: L2 tagging
        l2_result = self.stage2_tag(creator, l1_result)
        logger.info(
            f"  Stage 2 → {[t.tag for t in l2_result.tags]}"
        )

        return TaggingOutput(
            creator_id=creator.channel_id,
            l1_tags=l1_result.categories,
            l2_tags=l2_result.tags,
            model=self.model,
            prompt_version=self.prompt_version,
        )
