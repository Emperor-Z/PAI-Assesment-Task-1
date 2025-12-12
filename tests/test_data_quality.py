"""Tests for database metadata helpers focused on data quality."""

from __future__ import annotations

import json
import unittest
from typing import Dict

from src.database import DatabaseManager
from src.models import SurveyResponse


class TestDataQualityMetadata(unittest.TestCase):
    """Verify counts and rejection reason helpers for transparency."""

    def setUp(self) -> None:
        self.db_manager = DatabaseManager(":memory:")
        self.db_manager.connect()
        self.db_manager.create_tables()

    def tearDown(self) -> None:
        self.db_manager.close()

    def _make_response(self) -> SurveyResponse:
        genre_frequencies: Dict[str, int] = {"Lofi": 3, "Pop": 1}
        return SurveyResponse(
            timestamp="2022-08-27 19:29:02",
            age=21,
            primary_streaming_service="Spotify",
            hours_per_day=4.5,
            while_working=True,
            instrumentalist=False,
            composer=False,
            fav_genre="Lofi",
            exploratory=True,
            foreign_languages=False,
            bpm=90,
            anxiety_score=5,
            depression_score=4,
            insomnia_score=3,
            ocd_score=2,
            music_effects="Helps",
            genre_frequencies=genre_frequencies,
        )

    def _insert_rejected_rows(self) -> None:
        cursor = self.db_manager.connection.cursor()
        rejected_payloads = [
            (None, "missing age", {"Age": "", "Hours per day": "2"}),
            (None, "missing age", {"Age": "", "Hours per day": "1"}),
            (None, "invalid score", {"Anxiety": "eleven"}),
        ]
        for raw_row_id, reason, payload in rejected_payloads:
            cursor.execute(
                """
                INSERT INTO RejectedRows (raw_row_id, reason, raw_payload, created_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (raw_row_id, reason, json.dumps(payload)),
            )
        self.db_manager.connection.commit()

    def test_counts_are_reported_for_each_stage(self) -> None:
        """Counts for raw, clean, and rejected records should be integers."""

        raw_row = {"Age": "21", "Primary streaming service": "Spotify"}
        for _ in range(3):
            self.db_manager.insert_raw_response(raw_row, error=None)

        response = self._make_response()
        self.db_manager.insert_survey_response(response)
        self.db_manager.insert_survey_response(response)

        self._insert_rejected_rows()

        self.assertEqual(3, self.db_manager.get_raw_row_count())
        self.assertEqual(2, self.db_manager.get_clean_row_count())
        self.assertEqual(3, self.db_manager.get_rejected_row_count())

    def test_top_rejection_reasons_sorted_by_count(self) -> None:
        """Rejection reasons helper should return sorted reason and count pairs."""

        self._insert_rejected_rows()

        top_reasons = self.db_manager.get_top_rejection_reasons(limit=2)
        self.assertEqual([
            ("missing age", 2),
            ("invalid score", 1),
        ], top_reasons)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
