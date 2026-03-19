"""
Evaluation script: compute Precision / Recall / F1 for tagging quality.
Compares LLM predictions against ground truth (few-shot examples or manual annotations).
"""

import json
import sys
from collections import defaultdict
from pathlib import Path


def load_predictions(path: str) -> dict:
    """Load tagging results from pipeline output."""
    with open(path) as f:
        return json.load(f)


def load_ground_truth(path: str) -> list[dict]:
    """Load ground truth annotations (few-shot examples format)."""
    with open(path) as f:
        return json.load(f)


def compute_metrics(predicted_tags: set, actual_tags: set) -> dict:
    """Compute precision, recall, F1 for a single creator."""
    if not predicted_tags and not actual_tags:
        return {"precision": 1.0, "recall": 1.0, "f1": 1.0}
    if not predicted_tags:
        return {"precision": 0.0, "recall": 0.0, "f1": 0.0}
    if not actual_tags:
        return {"precision": 0.0, "recall": 0.0, "f1": 0.0}

    tp = len(predicted_tags & actual_tags)
    precision = tp / len(predicted_tags) if predicted_tags else 0
    recall = tp / len(actual_tags) if actual_tags else 0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0

    return {"precision": precision, "recall": recall, "f1": f1}


def evaluate_against_ground_truth(
    ground_truth: list[dict],
    predictions: dict,
) -> dict:
    """
    Evaluate tagging predictions against ground truth.

    Args:
        ground_truth: List of {creator_name, expected_tags: {L1: [...], L2: [...]}}
        predictions: Dict of {creator_name: {l1_tags: [...], l2_tags: [...]}}
    """
    l1_metrics = []
    l2_metrics = []
    per_creator = []

    for gt in ground_truth:
        name = gt["creator_name"]
        pred = predictions.get(name)
        if not pred:
            continue

        # L1 evaluation
        actual_l1 = {t["tag"] for t in gt["expected_tags"].get("L1", [])}
        pred_l1 = set(pred.get("l1_tags", []))
        l1_m = compute_metrics(pred_l1, actual_l1)
        l1_metrics.append(l1_m)

        # L2 evaluation
        actual_l2 = {t["tag"] for t in gt["expected_tags"].get("L2", [])}
        pred_l2 = set(pred.get("l2_tags", []))
        l2_m = compute_metrics(pred_l2, actual_l2)
        l2_metrics.append(l2_m)

        per_creator.append({
            "creator": name,
            "l1": {"predicted": sorted(pred_l1), "actual": sorted(actual_l1), **l1_m},
            "l2": {"predicted": sorted(pred_l2), "actual": sorted(actual_l2), **l2_m},
        })

    # Macro-average
    def avg_metric(metrics: list[dict], key: str) -> float:
        return sum(m[key] for m in metrics) / len(metrics) if metrics else 0

    return {
        "n_evaluated": len(l1_metrics),
        "l1_macro": {
            "precision": avg_metric(l1_metrics, "precision"),
            "recall": avg_metric(l1_metrics, "recall"),
            "f1": avg_metric(l1_metrics, "f1"),
        },
        "l2_macro": {
            "precision": avg_metric(l2_metrics, "precision"),
            "recall": avg_metric(l2_metrics, "recall"),
            "f1": avg_metric(l2_metrics, "f1"),
        },
        "per_creator": per_creator,
    }


def evaluate_from_mock_data(
    creators_path: str = "data/seed_creators.json",
    predictions_path: str = "data/predictions.json",
):
    """
    Evaluate using mock data's primary_categories as ground truth.
    This tests if the LLM correctly identifies the category used to generate the data.
    """
    with open(creators_path) as f:
        creators = json.load(f)

    try:
        with open(predictions_path) as f:
            predictions = json.load(f)
    except FileNotFoundError:
        print(f"No predictions file found at {predictions_path}")
        print("Run the pipeline first: python -m src.pipeline.batch_runner")
        return

    l1_metrics = []
    confusion = defaultdict(lambda: defaultdict(int))

    for creator in creators:
        cid = creator["channel_id"]
        pred = predictions.get(cid)
        if not pred:
            continue

        actual_l1 = set(creator.get("primary_categories", []))
        pred_l1 = set(pred.get("l1_tags", []))

        m = compute_metrics(pred_l1, actual_l1)
        l1_metrics.append(m)

        # Build confusion matrix
        for actual in actual_l1:
            for predicted in pred_l1:
                confusion[actual][predicted] += 1

    if not l1_metrics:
        print("No matching predictions found.")
        return

    avg_p = sum(m["precision"] for m in l1_metrics) / len(l1_metrics)
    avg_r = sum(m["recall"] for m in l1_metrics) / len(l1_metrics)
    avg_f1 = sum(m["f1"] for m in l1_metrics) / len(l1_metrics)

    print(f"\n{'='*50}")
    print(f"Evaluation Results (n={len(l1_metrics)})")
    print(f"{'='*50}")
    print(f"L1 Macro Precision: {avg_p:.3f}")
    print(f"L1 Macro Recall:    {avg_r:.3f}")
    print(f"L1 Macro F1:        {avg_f1:.3f}")
    print(f"\nConfusion Matrix (actual → predicted):")
    print(f"{'='*50}")

    all_cats = sorted(set(list(confusion.keys()) + [
        c for row in confusion.values() for c in row.keys()
    ]))

    # Print header
    header = f"{'':>20}" + "".join(f"{c[:8]:>10}" for c in all_cats)
    print(header)
    for actual in all_cats:
        row = f"{actual[:20]:>20}"
        for predicted in all_cats:
            count = confusion[actual][predicted]
            row += f"{count:>10}"
        print(row)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--mock":
        evaluate_from_mock_data()
    else:
        # Evaluate against few-shot ground truth
        gt = load_ground_truth("data/few_shot_examples.json")
        # You'd load actual predictions here
        print("Usage:")
        print("  python scripts/evaluate.py --mock    # Evaluate against mock data ground truth")
        print("  (Requires running the pipeline first to generate predictions.json)")
