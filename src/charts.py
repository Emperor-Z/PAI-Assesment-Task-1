"""Matplotlib chart rendering helpers."""

from __future__ import annotations

import io
from typing import Dict

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
