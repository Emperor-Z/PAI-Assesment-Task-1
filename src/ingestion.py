"""
CSV ingestion and data cleaning for the Music & Mental Health Insights Tool.

This module is responsible for:
- Reading the raw CSV file.
- Converting rows into SurveyResponse domain objects.
- Handling type conversions and frequency mappings.
"""

from __future__ import annotations

import csv
import logging
from typing import Dict, List, TYPE_CHECKING

from src.models import SurveyResponse, map_frequency_to_numeric

if TYPE_CHECKING:
    from src.database import DatabaseManager


def _parse_int_required(value: str, field_name: str) -> int:
    stripped = (value or "").strip()
    if stripped == "":
        raise ValueError(f"{field_name} is required but was empty.")
    try:
        return int(stripped)
    except ValueError as exc:
        raise ValueError(f"Invalid integer for {field_name}: {value!r}") from exc


def _parse_float_required(value: str, field_name: str) -> float:
    stripped = (value or "").strip()
    if stripped == "":
        raise ValueError(f"{field_name} is required but was empty.")
    try:
        return float(stripped)
    except ValueError as exc:
        raise ValueError(f"Invalid float for {field_name}: {value!r}") from exc


def _parse_int_or_none(value: str) -> int | None:
    stripped = (value or "").strip()
    if stripped == "":
        return None
    try:
        return int(stripped)
    except ValueError as exc:
        raise ValueError(f"Invalid optional integer value: {value!r}") from exc


def _parse_bool(value: str) -> bool:
    stripped = (value or "").strip().lower()
    return stripped in {"yes", "true", "y", "1"}


def _extract_genre_frequencies(row: Dict[str, str]) -> Dict[str, int]:
    frequencies: Dict[str, int] = {}
    for column, value in row.items():
        if not column.startswith("Frequency [") or not column.endswith("]"):
            continue
        genre = column[len("Frequency [") : -1]
        frequencies[genre] = map_frequency_to_numeric(value or "")
    return frequencies


def _row_to_survey_response(row: Dict[str, str]) -> SurveyResponse:
    return SurveyResponse(
        timestamp=row.get("Timestamp", ""),
        age=_parse_int_required(row.get("Age", ""), "Age"),
        primary_streaming_service=row.get("Primary streaming service", ""),
        hours_per_day=_parse_float_required(row.get("Hours per day", ""), "Hours per day"),
        while_working=_parse_bool(row.get("While working", "")),
        instrumentalist=_parse_bool(row.get("Instrumentalist", "")),
        composer=_parse_bool(row.get("Composer", "")),
        fav_genre=row.get("Fav genre", ""),
        exploratory=_parse_bool(row.get("Exploratory", "")),
        foreign_languages=_parse_bool(row.get("Foreign languages", "")),
        bpm=_parse_int_or_none(row.get("BPM", "")),
        anxiety_score=_parse_int_required(row.get("Anxiety", ""), "Anxiety"),
        depression_score=_parse_int_required(row.get("Depression", ""), "Depression"),
        insomnia_score=_parse_int_required(row.get("Insomnia", ""), "Insomnia"),
        ocd_score=_parse_int_required(row.get("OCD", ""), "OCD"),
        music_effects=row.get("Music effects", ""),
        genre_frequencies=_extract_genre_frequencies(row),
    )


LOGGER = logging.getLogger(__name__)


def load_survey_responses_from_csv(csv_path: str, strict: bool = True) -> List[SurveyResponse]:
    """
    Load survey responses from a CSV file path and return a list of
    SurveyResponse objects.
    """
    responses: List[SurveyResponse] = []
    with open(csv_path, newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            try:
                responses.append(_row_to_survey_response(row))
            except (ValueError, TypeError) as exc:
                if strict:
                    raise
                LOGGER.warning("Skipping invalid row: %s", exc)
                continue
    return responses


def ingest_csv_into_database(csv_path: str, db_manager: "DatabaseManager") -> int:
    """
    Load SurveyResponse objects from CSV and insert them into the database.

    Returns the number of inserted respondents.
    """
    responses = load_survey_responses_from_csv(csv_path)
    inserted = 0
    for response in responses:
        db_manager.insert_survey_response(response)
        inserted += 1
    return inserted
