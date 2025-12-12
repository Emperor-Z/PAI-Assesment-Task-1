"""Tests for tolerant ingestion mode."""

import csv
import os
import tempfile
import unittest

from src.ingestion import load_survey_responses_from_csv  # type: ignore[import]


class TestIngestionTolerant(unittest.TestCase):
    def test_load_survey_responses_tolerant_skips_invalid_rows(self) -> None:
        """load_survey_responses_from_csv(strict=False) should skip dirty rows."""
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = os.path.join(tmpdir, "mixed.csv")
            fieldnames = [
                "Timestamp",
                "Age",
                "Primary streaming service",
                "Hours per day",
                "While working",
                "Instrumentalist",
                "Composer",
                "Fav genre",
                "Exploratory",
                "Foreign languages",
                "BPM",
                "Frequency [Pop]",
                "Anxiety",
                "Depression",
                "Insomnia",
                "OCD",
                "Music effects",
            ]
            with open(csv_path, "w", newline="", encoding="utf-8") as csv_file:
                writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerow(
                    {
                        "Timestamp": "2022-08-27",
                        "Age": "21",
                        "Primary streaming service": "Spotify",
                        "Hours per day": "3.5",
                        "While working": "Yes",
                        "Instrumentalist": "No",
                        "Composer": "No",
                        "Fav genre": "Lofi",
                        "Exploratory": "Yes",
                        "Foreign languages": "No",
                        "BPM": "90",
                        "Frequency [Pop]": "Sometimes",
                        "Anxiety": "4",
                        "Depression": "3",
                        "Insomnia": "2",
                        "OCD": "1",
                        "Music effects": "Helps",
                    }
                )
                writer.writerow(
                    {
                        "Timestamp": "2022-08-28",
                        "Age": "",
                        "Primary streaming service": "YouTube",
                        "Hours per day": "1.0",
                        "While working": "No",
                        "Instrumentalist": "No",
                        "Composer": "No",
                        "Fav genre": "Pop",
                        "Exploratory": "No",
                        "Foreign languages": "No",
                        "BPM": "",
                        "Frequency [Pop]": "Rarely",
                        "Anxiety": "2",
                        "Depression": "1",
                        "Insomnia": "1",
                        "OCD": "0",
                        "Music effects": "Neutral",
                    }
                )

            responses = load_survey_responses_from_csv(csv_path, strict=False)
            self.assertEqual(1, len(responses))
