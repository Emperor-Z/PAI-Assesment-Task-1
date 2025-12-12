import unittest
from typing import List

from src.database import DatabaseManager
from src.filters import FilterCriteria
from src.models import SurveyResponse  # type: ignore[import]
from src.services import InsightsService  # type: ignore[import]


class TestInsightsService(unittest.TestCase):
    def setUp(self) -> None:
        self.db_manager = DatabaseManager(":memory:")
        self.db_manager.connect()
        self.db_manager.create_tables()
        self.responses: List[SurveyResponse] = [
            SurveyResponse(
                timestamp="t1",
                age=21,
                primary_streaming_service="Spotify",
                hours_per_day=4.0,
                while_working=True,
                instrumentalist=False,
                composer=False,
                fav_genre="Lofi",
                exploratory=True,
                foreign_languages=True,
                bpm=80,
                anxiety_score=5,
                depression_score=4,
                insomnia_score=6,
                ocd_score=2,
                music_effects="Improve",
                genre_frequencies={"Lofi": 3, "Pop": 2},
            ),
            SurveyResponse(
                timestamp="t2",
                age=30,
                primary_streaming_service="YouTube",
                hours_per_day=1.0,
                while_working=False,
                instrumentalist=True,
                composer=False,
                fav_genre="Pop",
                exploratory=False,
                foreign_languages=False,
                bpm=120,
                anxiety_score=3,
                depression_score=2,
                insomnia_score=1,
                ocd_score=0,
                music_effects="No effect",
                genre_frequencies={"Pop": 2},
            ),
        ]
        for response in self.responses:
            self.db_manager.insert_survey_response(response)
        self.service = InsightsService(self.db_manager)

    def tearDown(self) -> None:
        self.db_manager.close()

    def test_get_average_anxiety_and_depression_by_genre(self) -> None:
        """
        GIVEN a dataset with known scores per genre
        WHEN querying for Lofi
        THEN averages should match the data.
        """
        result = self.service.get_average_anxiety_and_depression_by_genre("Lofi")

        self.assertEqual("Lofi", result["genre"])
        self.assertAlmostEqual(5.0, result["avg_anxiety"])
        self.assertAlmostEqual(4.0, result["avg_depression"])

    def test_get_streaming_service_counts(self) -> None:
        counts = self.service.get_streaming_service_counts()
        self.assertEqual(1, counts.get("Spotify"))
        self.assertEqual(1, counts.get("YouTube"))

    def test_get_hours_vs_anxiety(self) -> None:
        buckets = self.service.get_hours_vs_anxiety()
        self.assertIn("<=1", buckets)
        self.assertIn(">3", buckets)

    def test_get_overview_with_filters(self) -> None:
        """Overview should respect filters and report mean scores."""
        criteria = FilterCriteria(streaming_service="Spotify")
        overview = self.service.get_overview(criteria)

        self.assertEqual(1, overview["total_count"])
        self.assertEqual("Spotify", overview["top_streaming_service"])

        scores = overview["mean_scores"]
        self.assertAlmostEqual(5.0, scores["anxiety"])
        self.assertAlmostEqual(4.0, scores["depression"])
        self.assertAlmostEqual(6.0, scores["insomnia"])
        self.assertAlmostEqual(2.0, scores["ocd"])
