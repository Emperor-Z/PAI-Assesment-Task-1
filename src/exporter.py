"""
CSV export utilities for the Music & Mental Health Insights Tool.
"""

from __future__ import annotations

import csv

from src.services import InsightsService


def export_streaming_counts_to_csv(output_path: str, service: InsightsService) -> None:
    """
    Export streaming service usage counts to a CSV file.

    """
    counts = service.get_streaming_service_counts()

    with open(output_path, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["service", "count"])
        for service_name, count in counts.items():
            writer.writerow([service_name, count])
