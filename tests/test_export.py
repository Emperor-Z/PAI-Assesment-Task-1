import csv
import os
import tempfile
import unittest
from typing import List

from src.exporter import export_streaming_counts_to_csv  # type: ignore[import]
from src.models import SurveyResponse  # type: ignore[import]
from src.services import InsightsService  # type: ignore[import]


class TestExporter(unittest.TestCase):
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
            SurveyResponse(
                timestamp="t3",
                age=25,
                primary_streaming_service="Spotify",
                hours_per_day=2.0,
                while_working=True,
                instrumentalist=False,
                composer=False,
                fav_genre="Rock",
                exploratory=True,
                foreign_languages=False,
                bpm=100,
                anxiety_score=4,
                depression_score=3,
                insomnia_score=2,
                ocd_score=1,
                music_effects="Neutral",
                genre_frequencies={"Rock": 2},
            ),
        ]
        self.service = InsightsService(self.responses)

    def test_export_streaming_counts_to_csv_writes_expected_rows(self) -> None:
        """
        GIVEN an InsightsService with known streaming usage
        WHEN export_streaming_counts_to_csv is called
        THEN a CSV file is created with the correct header and rows.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = os.path.join(tmpdir, "streaming_counts.csv")

            export_streaming_counts_to_csv(output_path, self.service)

            self.assertTrue(os.path.exists(output_path))

            with open(output_path, newline="", encoding="utf-8") as csv_file:
                reader = csv.DictReader(csv_file)
                rows = list(reader)
                fieldnames = reader.fieldnames or []

            self.assertEqual({"service", "count"}, set(fieldnames))
            counts = {row["service"]: int(row["count"]) for row in rows}
            self.assertEqual(2, counts.get("Spotify"))
            self.assertEqual(1, counts.get("YouTube"))
