"""Tests for cleaning staged raw responses into curated tables."""

import unittest

from src.cleaning import clean_raw_row_to_survey_response  # noqa: F401
from src.database import DatabaseManager  # type: ignore[import]
from src.etl_clean import clean_raw_responses_into_database  # type: ignore[import]


class TestEtLCleanLoad(unittest.TestCase):
    def setUp(self) -> None:
        self.db = DatabaseManager(":memory:")
        self.db.connect()
        self.db.create_tables()

    def tearDown(self) -> None:
        self.db.close()

    def test_cleaning_inserts_valid_rows_and_drops_invalid(self) -> None:
        valid_row = {
            "Timestamp": "2022-08-27",
            "Age": "21",
            "Primary streaming service": "Spotify",
            "Hours per day": "4.5",
            "While working": "Yes",
            "Instrumentalist": "No",
            "Composer": "No",
            "Fav genre": "Lofi",
            "Exploratory": "Yes",
            "Foreign languages": "No",
            "BPM": "80",
            "Frequency [Pop]": "Sometimes",
            "Anxiety": "5",
            "Depression": "4",
            "Insomnia": "3",
            "OCD": "2",
            "Music effects": "Helps",
        }
        invalid_row = dict(valid_row)
        invalid_row["Age"] = ""

        self.db.insert_raw_response(valid_row, error=None)
        self.db.insert_raw_response(invalid_row, error="Age missing")

        summary = clean_raw_responses_into_database(self.db)

        self.assertEqual(2, summary["raw_total"])
        self.assertEqual(1, summary["cleaned_inserted"])
        self.assertEqual(1, summary["cleaned_dropped"])
        self.assertEqual(1, self.db.get_respondent_count())
        self.assertEqual(1, len(self.db.get_all_health_stats_joined()))
