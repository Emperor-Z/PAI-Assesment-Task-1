"""Tests for the FilterCriteria helper."""

import unittest
from typing import Mapping

from src.filters import FilterCriteria


class FilterCriteriaDefaultsTests(unittest.TestCase):
    """Ensure default filter values are None."""

    def test_defaults_are_none(self) -> None:
        """Uninitialised filters should all be None."""
        criteria = FilterCriteria()
        self.assertIsNone(criteria.age_group)
        self.assertIsNone(criteria.streaming_service)
        self.assertIsNone(criteria.favourite_genre)
        self.assertIsNone(criteria.music_effects)
        self.assertIsNone(criteria.while_working)
        self.assertIsNone(criteria.exploratory)
        self.assertIsNone(criteria.foreign_languages)
        self.assertIsNone(criteria.instrumentalist)
        self.assertIsNone(criteria.composer)
        self.assertIsNone(criteria.hours_bucket)


class FilterCriteriaParsingTests(unittest.TestCase):
    """Validate parsing of request args."""

    def test_from_request_args_normalises_values(self) -> None:
        """Empty strings should become None and booleans parse from yes/no."""
        raw_args: Mapping[str, str] = {
            "age_group": "",
            "streaming_service": " Spotify ",
            "favourite_genre": "",
            "music_effects": "Focus",
            "while_working": "yes",
            "exploratory": "no",
            "foreign_languages": "YES",
            "instrumentalist": "NO",
            "composer": "",
            "hours_bucket": " 1-3 ",
        }

        criteria = FilterCriteria.from_request_args(raw_args)

        self.assertIsNone(criteria.age_group)
        self.assertEqual(criteria.streaming_service, "Spotify")
        self.assertIsNone(criteria.favourite_genre)
        self.assertEqual(criteria.music_effects, "Focus")
        self.assertTrue(criteria.while_working)
        self.assertFalse(criteria.exploratory)
        self.assertTrue(criteria.foreign_languages)
        self.assertFalse(criteria.instrumentalist)
        self.assertIsNone(criteria.composer)
        self.assertEqual(criteria.hours_bucket, "1-3")
