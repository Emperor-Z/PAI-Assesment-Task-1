"""Convert raw CSV rows into validated SurveyResponse objects."""

from __future__ import annotations

from typing import Dict, Optional

from src.models import SurveyResponse, map_frequency_to_numeric


def _normalise_str(value: str | None) -> str:
    return (value or "").strip()


def _parse_int_required(value: str | None, field: str) -> int:
    cleaned = _normalise_str(value)
    if not cleaned:
        raise ValueError(f"{field} is required but was empty")
    return int(cleaned)


def _parse_float_required(value: str | None, field: str) -> float:
    cleaned = _normalise_str(value)
    if not cleaned:
        raise ValueError(f"{field} is required but was empty")
    return float(cleaned)


def _parse_int_optional(value: str | None) -> Optional[int]:
    cleaned = _normalise_str(value)
    if not cleaned:
        return None
    return int(cleaned)


def _parse_bool_yes_no(value: str | None) -> bool:
    cleaned = _normalise_str(value).lower()
    return cleaned in {"yes", "true", "1", "y"}


def _extract_genre_frequencies(row: Dict[str, str]) -> Dict[str, int]:
    frequencies: Dict[str, int] = {}
    for key, val in row.items():
        if key.startswith("Frequency [") and key.endswith("]"):
            genre = key[len("Frequency [") : -1]
            score = map_frequency_to_numeric(val)
            frequencies[genre] = score
    return frequencies


def clean_raw_row_to_survey_response(raw_row: Dict[str, str]) -> Optional[SurveyResponse]:
    try:
        age = _parse_int_required(raw_row.get("Age"), "Age")
        hours = _parse_float_required(raw_row.get("Hours per day"), "Hours per day")
        bpm = _parse_int_optional(raw_row.get("BPM"))
        genre_freqs = _extract_genre_frequencies(raw_row)
        response = SurveyResponse(
            timestamp=_normalise_str(raw_row.get("Timestamp")),
            age=age,
            primary_streaming_service=_normalise_str(raw_row.get("Primary streaming service")),
            hours_per_day=hours,
            while_working=_parse_bool_yes_no(raw_row.get("While working")),
            instrumentalist=_parse_bool_yes_no(raw_row.get("Instrumentalist")),
            composer=_parse_bool_yes_no(raw_row.get("Composer")),
            fav_genre=_normalise_str(raw_row.get("Fav genre")),
            exploratory=_parse_bool_yes_no(raw_row.get("Exploratory")),
            foreign_languages=_parse_bool_yes_no(raw_row.get("Foreign languages")),
            bpm=bpm,
            anxiety_score=_parse_int_required(raw_row.get("Anxiety"), "Anxiety"),
            depression_score=_parse_int_required(raw_row.get("Depression"), "Depression"),
            insomnia_score=_parse_int_required(raw_row.get("Insomnia"), "Insomnia"),
            ocd_score=_parse_int_required(raw_row.get("OCD"), "OCD"),
            music_effects=_normalise_str(raw_row.get("Music effects")),
            genre_frequencies=genre_freqs,
        )
        return response
    except (ValueError, TypeError):
        return None
