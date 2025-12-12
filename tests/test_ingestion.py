"""
Unit tests for CSV ingestion and data cleaning.

These tests cover loading the Music & Mental Health survey data
from CSV into SurveyResponse domain objects.
"""

import os
import unittest

from src.database import DatabaseManager  # type: ignore[import]
from src.ingestion import ingest_csv_into_database, load_survey_responses_from_csv  # type: ignore[import]
from src.models import SurveyResponse  # type: ignore[import]


class TestIngestion(unittest.TestCase):
    def setUp(self) -> None:
        self.fixtures_dir = os.path.join(os.path.dirname(__file__), "data")
        self.sample_csv = os.path.join(self.fixtures_dir, "sample_mxmh.csv")

    def test_load_survey_responses_from_csv_returns_expected_count(self) -> None:
        """
        GIVEN a small sample CSV file
        WHEN load_survey_responses_from_csv is called
        THEN it should return a list of SurveyResponse objects with the expected length.
        """
        responses = load_survey_responses_from_csv(self.sample_csv)

        self.assertIsInstance(responses, list)
        self.assertGreaterEqual(len(responses), 2)
        self.assertIsInstance(responses[0], SurveyResponse)

    def test_type_conversion_and_frequency_mapping(self) -> None:
        """
        GIVEN a known row in the sample CSV
        WHEN loaded via load_survey_responses_from_csv
        THEN numeric and boolean fields should be correctly converted, and
             frequency columns should be mapped using map_frequency_to_numeric.
        """
        responses = load_survey_responses_from_csv(self.sample_csv)
        first = responses[0]

        # Scalars
        self.assertEqual(21, first.age)
        self.assertAlmostEqual(4.5, first.hours_per_day)
        self.assertTrue(first.while_working)
        self.assertFalse(first.instrumentalist)
        self.assertFalse(first.composer)
        self.assertEqual("Lofi", first.fav_genre)
        self.assertTrue(first.exploratory)
        self.assertTrue(first.foreign_languages)
        self.assertEqual(80, first.bpm)

        # Mental health scores
        self.assertEqual(5, first.anxiety_score)
        self.assertEqual(4, first.depression_score)
        self.assertEqual(6, first.insomnia_score)
        self.assertEqual(2, first.ocd_score)

        # Frequency mapping - we don't assert exact values, but we assert
        # that mapping produced integers for known genres.
        self.assertIn("Pop", first.genre_frequencies)
        self.assertIn("Lofi", first.genre_frequencies)
        self.assertIsInstance(first.genre_frequencies["Pop"], int)
        self.assertIsInstance(first.genre_frequencies["Lofi"], int)

    def test_missing_file_raises_file_not_found_error(self) -> None:
        """
        GIVEN a path to a non-existent CSV file
        WHEN load_survey_responses_from_csv is called
        THEN it should raise FileNotFoundError.
        """
        missing_path = os.path.join(self.fixtures_dir, "does_not_exist.csv")
        with self.assertRaises(FileNotFoundError):
            load_survey_responses_from_csv(missing_path)

    def test_ingest_csv_into_database(self) -> None:
        """
        GIVEN a sample CSV and an in-memory DatabaseManager
        WHEN ingest_csv_into_database is called
        THEN the number of inserted respondents should match the CSV rows.
        """
        db_manager = DatabaseManager(":memory:")
        db_manager.connect()
        db_manager.create_tables()
        try:
            inserted = ingest_csv_into_database(self.sample_csv, db_manager)
            self.assertEqual(2, inserted)
            self.assertEqual(2, db_manager.get_respondent_count())
        finally:
            db_manager.close()
