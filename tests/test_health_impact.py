"""Tests for InsightsService health impact analytics."""

from __future__ import annotations

import unittest
from typing import List

from src.database import DatabaseManager
from src.models import SurveyResponse
from src.services import InsightsService


class TestHealthImpactAnalytics(unittest.TestCase):
    """Verify grouped means analytics for health impact reporting."""

    def setUp(self) -> None:
        self.db = DatabaseManager(":memory:")
        self.db.connect()
        self.db.create_tables()
        self._seed_data()
        self.service = InsightsService(self.db)

    def tearDown(self) -> None:
        self.db.close()

    def _seed_data(self) -> None:
        """Insert deterministic respondents covering multiple groups."""
        rows: List[SurveyResponse] = []
        for idx in range(10):
            rows.append(
                SurveyResponse(
                    timestamp=f"2022-08-2{idx}",
                    age=20 + idx,
                    primary_streaming_service="Spotify" if idx % 2 == 0 else "YouTube",
                    hours_per_day=1.0 + (idx % 3),
                    while_working=idx % 2 == 0,
                    instrumentalist=False,
                    composer=False,
                    fav_genre="Lofi" if idx < 5 else "Rock",
                    exploratory=True,
                    foreign_languages=False,
                    bpm=90,
                    anxiety_score=2 + idx % 5,
                    depression_score=2 + (idx + 1) % 4,
                    insomnia_score=1 + (idx % 3),
                    ocd_score=idx % 4,
                    music_effects="Helps" if idx < 6 else "Neutral",
                    genre_frequencies={},
                )
            )
        for row in rows:
            self.db.insert_survey_response(row)

    def test_mean_scores_by_genre(self) -> None:
        results = self.service.get_mean_scores_by_genre(top_n=10, min_n=1)
        self.assertGreaterEqual(len(results), 2)
        first = results[0]
        self.assertIn("genre", first)
        self.assertIn("n", first)
        self.assertIn("anxiety_mean", first)
        self.assertEqual(5, first["n"])

    def test_mean_scores_by_music_effects(self) -> None:
        results = self.service.get_mean_scores_by_music_effects(min_n=1)
        labels = {row["effect"] for row in results}
        self.assertSetEqual(labels, {"Helps", "Neutral"})
        for row in results:
            self.assertIn("anxiety_mean", row)
            self.assertIn("depression_mean", row)

    def test_mean_scores_by_hours_bucket(self) -> None:
        results = self.service.get_mean_scores_by_hours_bucket(min_n=1)
        labels = {row["bucket"] for row in results}
        self.assertTrue({"<=1", "1-3", ">3"}.intersection(labels))
        for row in results:
            self.assertIn("n", row)
            self.assertGreater(row["n"], 0)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
