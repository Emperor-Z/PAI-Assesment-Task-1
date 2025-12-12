"""Matplotlib chart rendering helpers."""

from __future__ import annotations

import io
from typing import Dict, List, Sequence

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


def render_streaming_counts_png(counts: Dict[str, int]) -> bytes:
    fig, ax = plt.subplots(figsize=(6, 4))
    services = list(counts.keys()) or ["No data"]
    values = [counts.get(service, 0) for service in services]
    ax.bar(services, values, color="#4F81BD")
    ax.set_title("Streaming service usage")
    ax.set_ylabel("Respondents")
    ax.set_xlabel("Service")
    fig.tight_layout()
    buffer = io.BytesIO()
    fig.savefig(buffer, format="png")
    plt.close(fig)
    buffer.seek(0)
    return buffer.getvalue()


def render_hours_vs_anxiety_png(buckets: Dict[str, float]) -> bytes:
    fig, ax = plt.subplots(figsize=(6, 4))
    ordered_labels = ["<=1", "1-3", ">3"]
    labels = [label for label in ordered_labels if label in buckets] or list(buckets.keys()) or ["No data"]
    values = [buckets.get(label, 0.0) for label in labels]
    ax.bar(labels, values, color="#C0504D")
    ax.set_title("Listening duration vs average anxiety")
    ax.set_ylabel("Average anxiety score")
    ax.set_xlabel("Listening hours bucket")
    fig.tight_layout()
    buffer = io.BytesIO()
    fig.savefig(buffer, format="png")
    plt.close(fig)
    buffer.seek(0)
    return buffer.getvalue()


def render_score_distribution_chart(metric_label: str, distribution: Dict[int, int]) -> bytes:
    """Render a bar chart for a metric's score distribution."""
    fig, ax = plt.subplots(figsize=(6, 4))
    scores: List[int] = sorted(distribution.keys())
    values = [distribution.get(score, 0) for score in scores]
    ax.bar(scores, values, color="#9BBB59", width=0.8)
    ax.set_title(f"{metric_label} distribution")
    ax.set_xlabel("Score")
    ax.set_ylabel("Respondents")
    ax.set_xticks(scores)
    fig.tight_layout()
    buffer = io.BytesIO()
    fig.savefig(buffer, format="png")
    plt.close(fig)
    buffer.seek(0)
    return buffer.getvalue()


def render_age_group_means_chart(means: Dict[str, Dict[str, float]]) -> bytes:
    """Render grouped bar chart for age bucket mean scores."""
    metrics = ["anxiety", "depression", "insomnia", "ocd"]
    colors = ["#4F81BD", "#C0504D", "#9BBB59", "#8064A2"]
    groups = list(means.keys()) or ["No data"]

    fig, ax = plt.subplots(figsize=(8, 4.5))
    indices = range(len(groups))
    width = 0.18

    for idx, metric in enumerate(metrics):
        offsets = [i + (idx - 1.5) * width for i in indices]
        values = [means.get(group, {}).get(metric, 0.0) for group in groups]
        ax.bar(offsets, values, width=width, label=metric.title(), color=colors[idx])

    ax.set_xticks(list(indices))
    ax.set_xticklabels(groups, rotation=20, ha="right")
    ax.set_ylabel("Average score")
    ax.set_title("Age group mean scores")
    ax.legend()
    fig.tight_layout()

    buffer = io.BytesIO()
    fig.savefig(buffer, format="png")
    plt.close(fig)
    buffer.seek(0)
    return buffer.getvalue()


def render_music_effects_chart(counts: Dict[str, int]) -> bytes:
    """Render a bar chart for music effects labels."""
    labels = list(counts.keys()) or ["No data"]
    values = [counts.get(label, 0) for label in labels]

    fig, ax = plt.subplots(figsize=(6, 4))
    ax.barh(labels, values, color="#F79646")
    ax.set_xlabel("Respondents")
    ax.set_title("Music effects reported")
    fig.tight_layout()

    buffer = io.BytesIO()
    fig.savefig(buffer, format="png")
    plt.close(fig)
    buffer.seek(0)
    return buffer.getvalue()


def render_top_genres_chart(top_genres: Sequence[tuple[str, int]]) -> bytes:
    """Render a bar chart for the most popular genres."""
    labels = [genre for genre, _ in top_genres] or ["No data"]
    values = [count for _, count in top_genres] or [0]
    indices = list(range(len(labels)))

    fig, ax = plt.subplots(figsize=(7, 4))
    ax.bar(indices, values, color="#4BACC6")
    ax.set_ylabel("Respondents")
    ax.set_title("Top favourite genres")
    ax.set_xticks(indices)
    ax.set_xticklabels(labels, rotation=30, ha="right")
    fig.tight_layout()

    buffer = io.BytesIO()
    fig.savefig(buffer, format="png")
    plt.close(fig)
    buffer.seek(0)
    return buffer.getvalue()


def render_genre_vs_anxiety_chart(rows: Sequence[Dict[str, float | int | str]]) -> bytes:
    """Render average anxiety scores by genre."""
    labels = [str(row.get("genre", "Unknown")) for row in rows] or ["No data"]
    values = [float(row.get("anxiety_mean", 0.0)) for row in rows] or [0.0]
    counts = [int(row.get("n", 0)) for row in rows]
    fig, ax = plt.subplots(figsize=(7, 4))
    bars = ax.bar(labels, values, color="#4F81BD")
    for bar, n in zip(bars, counts):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(), f"n={n}", ha="center", va="bottom", fontsize=8)
    ax.set_ylabel("Average anxiety")
    ax.set_title("Anxiety by favourite genre")
    plt.setp(ax.get_xticklabels(), rotation=30, ha="right")
    fig.tight_layout()
    buffer = io.BytesIO()
    fig.savefig(buffer, format="png")
    plt.close(fig)
    buffer.seek(0)
    return buffer.getvalue()


def render_effects_vs_anxiety_chart(rows: Sequence[Dict[str, float | int | str]]) -> bytes:
    """Render anxiety averages by music effect."""
    labels = [str(row.get("effect", "Unknown")) for row in rows] or ["No data"]
    values = [float(row.get("anxiety_mean", 0.0)) for row in rows] or [0.0]
    counts = [int(row.get("n", 0)) for row in rows]
    fig, ax = plt.subplots(figsize=(7, 4))
    bars = ax.bar(labels, values, color="#C0504D")
    for bar, n in zip(bars, counts):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(), f"n={n}", ha="center", va="bottom", fontsize=8)
    ax.set_ylabel("Average anxiety")
    ax.set_title("Anxiety by music effect")
    plt.setp(ax.get_xticklabels(), rotation=30, ha="right")
    fig.tight_layout()
    buffer = io.BytesIO()
    fig.savefig(buffer, format="png")
    plt.close(fig)
    buffer.seek(0)
    return buffer.getvalue()


def render_hours_vs_scores_chart(rows: Sequence[Dict[str, float | int | str]]) -> bytes:
    """Render a multi-line chart for hours bucket vs mean scores."""
    if not rows:
        default_buckets = ["<=1", "1-3", ">3"]
        rows = [{"bucket": b, "anxiety_mean": 0.0, "depression_mean": 0.0, "insomnia_mean": 0.0, "ocd_mean": 0.0} for b in default_buckets]
    buckets = [row.get("bucket", "") for row in rows]
    metrics = ["anxiety_mean", "depression_mean", "insomnia_mean", "ocd_mean"]
    labels = ["Anxiety", "Depression", "Insomnia", "OCD"]
    colors = ["#4F81BD", "#C0504D", "#9BBB59", "#8064A2"]

    fig, ax = plt.subplots(figsize=(7, 4))
    for metric, label, color in zip(metrics, labels, colors):
        values = [row.get(metric, 0.0) for row in rows]
        ax.plot(buckets, values, marker="o", label=label, color=color)

    ax.set_ylabel("Average score")
    ax.set_xlabel("Listening hours bucket")
    ax.set_title("Hours per day vs mental health scores")
    ax.legend()
    fig.tight_layout()
    buffer = io.BytesIO()
    fig.savefig(buffer, format="png")
    plt.close(fig)
    buffer.seek(0)
    return buffer.getvalue()
