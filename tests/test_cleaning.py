"""Tests for raw row cleaning into SurveyResponse objects."""

import unittest

from src.cleaning import clean_raw_row_to_survey_response  # type: ignore[import]


class TestCleaning(unittest.TestCase):
    def setUp(self) -> None:
        self.valid_row = {
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

    def test_missing_required_age_returns_none(self) -> None:
        raw = dict(self.valid_row)
        raw["Age"] = ""
        result = clean_raw_row_to_survey_response(raw)
        self.assertIsNone(result)

    def test_valid_row_returns_survey_response(self) -> None:
        response = clean_raw_row_to_survey_response(self.valid_row)
        self.assertIsNotNone(response)
        assert response is not None
        self.assertEqual(21, response.age)
        self.assertAlmostEqual(4.5, response.hours_per_day)
        self.assertTrue(response.while_working)
        self.assertFalse(response.instrumentalist)
        self.assertIsInstance(response.genre_frequencies, dict)
        self.assertEqual(5, response.anxiety_score)
        self.assertEqual(4, response.depression_score)
        self.assertEqual(3, response.insomnia_score)
        self.assertEqual(2, response.ocd_score)
        self.assertEqual({"Pop": 2}, response.genre_frequencies)

    def test_unknown_frequency_label_drops_row(self) -> None:
        raw = dict(self.valid_row)
        raw["Frequency [Pop]"] = "InvalidLabel"
        result = clean_raw_row_to_survey_response(raw)
        self.assertIsNone(result)
