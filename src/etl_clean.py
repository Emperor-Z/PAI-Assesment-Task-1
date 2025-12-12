"""Cleaning staged raw responses into curated tables."""

from __future__ import annotations

from typing import Dict

from src.cleaning import clean_raw_row_to_survey_response
from src.database import DatabaseManager


def clean_raw_responses_into_database(db_manager: DatabaseManager) -> Dict[str, int]:
    """Clean staged raw responses and insert valid ones into the curated tables."""
    raw_rows = db_manager.get_all_raw_responses()
    raw_total = len(raw_rows)
    cleaned_inserted = 0
    cleaned_dropped = 0

    for raw_row in raw_rows:
        cleaned = clean_raw_row_to_survey_response(raw_row)
        if cleaned is None:
            cleaned_dropped += 1
            continue
        db_manager.insert_survey_response(cleaned)
        cleaned_inserted += 1

    return {
        "raw_total": raw_total,
        "cleaned_inserted": cleaned_inserted,
        "cleaned_dropped": cleaned_dropped,
    }
