"""Stage raw CSV rows into the RawResponses table."""

from __future__ import annotations

import csv
from typing import Dict

from src.database import DatabaseManager
from src.ingestion import _row_to_survey_response  # type: ignore[attr-defined]


def ingest_csv_into_raw_database(csv_path: str, db_manager: DatabaseManager) -> None:
    """Load CSV rows and store them in RawResponses with any errors."""
    with open(csv_path, newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            error: str | None = None
            try:
                _row_to_survey_response(row)
            except (ValueError, TypeError) as exc:  # pragma: no cover - covered via tests
                error = str(exc)
            finally:
                db_manager.insert_raw_response(row, error)
