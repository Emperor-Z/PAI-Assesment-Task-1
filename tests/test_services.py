import unittest
from typing import List

from src.models import SurveyResponse  # type: ignore[import]
from src.services import InsightsService  # type: ignore[import]


class TestInsightsService(unittest.TestCase):
    def setUp(self) -> None:
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
                music_effects="Helps me cope",
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
                music_effects="Makes me happy",
                genre_frequencies={"Pop": 2},
            ),
        ]
        self.service = InsightsService(self.responses)

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
