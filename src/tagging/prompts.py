"""Prompt templates for 2-stage LLM tagging pipeline."""

SYSTEM_PROMPT = """You are an expert content classifier for a YouTube influencer marketing platform.
Your job is to accurately categorize YouTube creators based on their channel information.
Be precise and conservative — only assign tags you are confident about.
Always return valid JSON matching the requested schema."""

STAGE1_PROMPT = """Classify this YouTube creator into 1-3 broad categories (L1 tags).

Available L1 Categories:
{taxonomy_l1}

Creator Information:
- Channel Name: {name}
- Channel Description: {description}
- Recent Video Titles:
{video_titles}

{few_shot_section}

Return a JSON object with this exact structure:
{{
  "categories": [
    {{"tag": "<L1 category name>", "confidence": <0.0-1.0>}}
  ]
}}

Rules:
- Only use category names from the provided list
- Assign 1-2 categories maximum
- Confidence should reflect how certain you are (0.7+ means quite confident, 0.8+ means really sure)
- Consider ALL video titles collectively, not just one
- If the channel description is vague, rely more on video titles"""

STAGE2_PROMPT = """Now assign specific subcategory tags (L2) for this creator.
The creator has been classified into these L1 categories: {l1_categories}

Available L2 tags for each category:
{taxonomy_l2}

Creator Information:
- Channel Name: {name}
- Channel Description: {description}
- Recent Video Titles:
{video_titles}

Return a JSON object with this exact structure:
{{
  "tags": [
    {{"tag": "<L2 tag name>", "confidence": <0.0-1.0>}}
  ],
  "candidate_tags": [
    {{"tag": "<L2 tag name>", "confidence": <0.0-1.0>}}
  ],
  "suggested_new_tags": [
    {{"suggested_tag": "<new tag name>", "parent_l1": "<L1 category>", "reason": "<why this tag should exist>"}}
  ]
}}

Rules:
- "tags": your confident picks (1-5 tags, confidence >= 0.6)
- "candidate_tags": 2-5 additional tags that MIGHT also apply but you're less sure about (confidence 0.3-0.6). These are shown to human reviewers as suggestions they can add with one click
- Only use tag names from the provided L2 lists above for both "tags" and "candidate_tags"
- Each tag must belong to one of the predicted L1 categories
- If none of the available L2 tags accurately describe this creator's content niche, suggest a new tag in "suggested_new_tags"
- If the existing L2 tags are sufficient, return an empty array for "suggested_new_tags"
- A "Cooking_Tutorial" channel that also reviews restaurants could get both Cooking_Tutorial and Restaurant_Review"""


def format_taxonomy_l1(taxonomy_tree: dict) -> str:
    """Format L1 categories for the Stage 1 prompt."""
    lines = []
    for l1_name, l1_data in sorted(taxonomy_tree.items()):
        lines.append(f"- {l1_name}: {l1_data['description']}")
    return "\n".join(lines)


def format_taxonomy_l2(taxonomy_tree: dict, l1_categories: list[str]) -> str:
    """Format L2 tags under predicted L1 categories for Stage 2 prompt."""
    lines = []
    for l1_name in l1_categories:
        if l1_name in taxonomy_tree:
            lines.append(f"\n[{l1_name}]")
            for l2_name, l2_desc in sorted(taxonomy_tree[l1_name]["children"].items()):
                lines.append(f"  - {l2_name}: {l2_desc}")
    return "\n".join(lines)


def format_video_titles(titles: list[str]) -> str:
    """Format video titles as a numbered list."""
    return "\n".join(f"  {i+1}. {title}" for i, title in enumerate(titles))


def format_few_shot(examples: list[dict]) -> str:
    """Format few-shot examples for the prompt."""
    if not examples:
        return ""

    lines = ["Here are some examples of correct classifications:\n"]
    for ex in examples[:3]:  # Use max 3 examples to save tokens
        titles = ", ".join(ex["recent_video_titles"][:3])
        tags = ", ".join(t["tag"] for t in ex["expected_tags"]["L1"])
        lines.append(
            f"- {ex['creator_name']} (videos: {titles}...)\n"
            f"  → L1: {tags}"
        )
    lines.append("")
    return "\n".join(lines)
