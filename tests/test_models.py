"""
Unit tests for the domain model and analysis logic.

These tests are written BEFORE the actual implementation in src/models.py
in line with a strict Test-Driven Development (TDD) process:

    1. Write test (Red)
    2. See test fail
    3. Write minimal code (Green)
    4. Refactor with tests still green

Initially, imports from src.models will fail or methods will raise
NotImplementedError. That is expected at this stage.
"""

import unittest
from typing import Dict, List

# These imports will initially fail until stubs are added to src/models.py.
# That is part of the "Red" phase in TDD.
from src.models import (  # type: ignore[import]
    SurveyResponse,
    AnalysisEngine,
    map_frequency_to_numeric,
)


class TestFrequencyMapping(unittest.TestCase):
    """
    Tests for the helper function that converts textual listening
    frequency into numeric scores.

    This is part of the "data cleaning" responsibility and must be
    deterministic and well-defined, as it underpins many analytics.
    """

    def test_map_frequency_to_numeric_standard_values(self) -> None:
        """
        GIVEN standard textual frequency categories
        WHEN map_frequency_to_numeric is called
        THEN it should return consistent integer codes.

        Mapping contract (can be cited in the report):
        - "Never"           -> 0
        - "Rarely"          -> 1
        - "Sometimes"       -> 2
        - "Very frequently" -> 3
        """
        self.assertEqual(0, map_frequency_to_numeric("Never"))
        self.assertEqual(1, map_frequency_to_numeric("Rarely"))
        self.assertEqual(2, map_frequency_to_numeric("Sometimes"))
        self.assertEqual(3, map_frequency_to_numeric("Very frequently"))

    def test_map_frequency_to_numeric_is_case_insensitive_and_strips_spaces(self) -> None:
        """
        GIVEN inputs with varying case and surrounding whitespace
        WHEN map_frequency_to_numeric is called
        THEN it should normalise input (strip + lower) before mapping.
        """
        self.assertEqual(3, map_frequency_to_numeric("  VERY FREQUENTLY "))
        self.assertEqual(1, map_frequency_to_numeric("rarely"))
        self.assertEqual(2, map_frequency_to_numeric("   Sometimes   "))

    def test_map_frequency_to_numeric_unknown_value_raises_value_error(self) -> None:
        """
        GIVEN an unknown frequency label
        WHEN map_frequency_to_numeric is called
        THEN it should raise a ValueError rather than silently returning 0.

        This fail-fast behaviour makes data issues visible early and
        avoids incorrect analytics downstream.
        """
        with self.assertRaises(ValueError):
            map_frequency_to_numeric("Once in a blue moon")
        with self.assertRaises(ValueError):
            map_frequency_to_numeric("")


class TestSurveyResponseConstruction(unittest.TestCase):
    """
    Tests for the SurveyResponse domain entity.

    The SurveyResponse class represents a single respondent after the
    data has been cleaned and normalised. It aggregates many
    attributes but keeps genre listening frequencies grouped in a
    dedicated dictionary to avoid 'data clumping'.
    """

    def test_survey_response_initialisation_and_types(self) -> None:
        """
        GIVEN a set of cleaned, typed values representing a survey row
        WHEN a SurveyResponse object is constructed
        THEN all attributes should be stored with correct types and values.

        This test validates:
        - proper use of type hints and isinstance checks
        - avoidance of data clumping by using a genre_frequencies dict
        """
        genre_frequencies: Dict[str, int] = {
            "Classical": 0,
            "Rock": 3,
            "Pop": 2,
            "Lofi": 3,
            "Video game music": 1,
        }

        response = SurveyResponse(
            timestamp="2022-08-27 19:29:02",
            age=21,
            primary_streaming_service="Spotify",
            hours_per_day=4.5,
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
            genre_frequencies=genre_frequencies,
        )

        # Type checks
        self.assertIsInstance(response.age, int)
        self.assertIsInstance(response.primary_streaming_service, str)
        self.assertIsInstance(response.hours_per_day, float)
        self.assertIsInstance(response.while_working, bool)
        self.assertIsInstance(response.genre_frequencies, dict)

        # Value checks
        self.assertEqual(21, response.age)
        self.assertEqual("Spotify", response.primary_streaming_service)
        self.assertEqual("Lofi", response.fav_genre)
        self.assertEqual(5, response.anxiety_score)
        self.assertEqual(6, response.insomnia_score)
        self.assertEqual("Helps me cope", response.music_effects)

        # Genre frequencies checks
        self.assertIn("Rock", response.genre_frequencies)
        self.assertEqual(3, response.genre_frequencies["Rock"])
        self.assertEqual(2, response.genre_frequencies["Pop"])

    def test_survey_response_raises_type_error_for_invalid_types(self) -> None:
        """
        GIVEN invalid types for critical attributes (e.g. age as string)
        WHEN constructing SurveyResponse
        THEN a TypeError (or ValueError) should be raised.

        This enforces fail-fast, guard-clause style validation and
        demonstrates the use of isinstance checks in the constructor.
        """
        genre_frequencies: Dict[str, int] = {"Rock": 3}

        with self.assertRaises((TypeError, ValueError)):
            SurveyResponse(
                timestamp="2022-08-27 19:29:02",
                age="21",  # wrong type on purpose
                primary_streaming_service="Spotify",
                hours_per_day=4.5,
                while_working=True,
                instrumentalist=False,
                composer=False,
                fav_genre="Rock",
                exploratory=True,
                foreign_languages=False,
                bpm=120,
                anxiety_score=7,
                depression_score=5,
                insomnia_score=8,
                ocd_score=3,
                music_effects="Worsens my anxiety",
                genre_frequencies=genre_frequencies,
            )


class TestAnalysisEngine(unittest.TestCase):
    """
    Tests for the AnalysisEngine class, which encapsulates analytics
    over a collection of SurveyResponse objects.

    These tests are intentionally independent of CSV/database logic,
    focusing purely on in-memory behaviour.
    """

    def setUp(self) -> None:
        """
        Create a small, representative dataset of SurveyResponse
        objects before each test, to keep tests isolated and repeatable.
        """
        self.responses: List[SurveyResponse] = [
            SurveyResponse(
                timestamp="2022-08-27 19:29:02",
                age=18,
                primary_streaming_service="Spotify",
                hours_per_day=3.0,
                while_working=True,
                instrumentalist=False,
                composer=False,
                fav_genre="Pop",
                exploratory=True,
                foreign_languages=True,
                bpm=120,
                anxiety_score=3,
                depression_score=2,
                insomnia_score=1,
                ocd_score=1,
                music_effects="Helps me cope",
                genre_frequencies={
                    "Pop": 3,
                    "Lofi": 2,
                },
            ),
            SurveyResponse(
                timestamp="2022-08-27 20:00:00",
                age=25,
                primary_streaming_service="Spotify",
                hours_per_day=5.0,
                while_working=False,
                instrumentalist=True,
                composer=True,
                fav_genre="Rock",
                exploratory=False,
                foreign_languages=False,
                bpm=140,
                anxiety_score=7,
                depression_score=6,
                insomnia_score=8,
                ocd_score=5,
                music_effects="Worsens my anxiety",
                genre_frequencies={
                    "Rock": 3,
                    "Metal": 2,
                },
            ),
            SurveyResponse(
                timestamp="2022-08-27 21:30:00",
                age=30,
                primary_streaming_service="YouTube Music",
                hours_per_day=1.0,
                while_working=True,
                instrumentalist=False,
                composer=False,
                fav_genre="Lofi",
                exploratory=True,
                foreign_languages=True,
                bpm=90,
                anxiety_score=5,
                depression_score=4,
                insomnia_score=2,
                ocd_score=2,
                music_effects="No effect",
                genre_frequencies={
                    "Lofi": 3,
                    "Classical": 1,
                },
            ),
        ]
        self.engine = AnalysisEngine(self.responses)

    def test_get_average_anxiety_by_genre_pop(self) -> None:
        """
        GIVEN a dataset with two 'Pop' listeners (including Pop as favourite
        or high-frequency genre) and known anxiety scores
        WHEN get_average_anxiety_by_genre('Pop') is called
        THEN it should return the correct arithmetic mean.

        In this fixture, only the first respondent has fav_genre == 'Pop'
        with anxiety_score = 3, so the mean is 3.0.
        """
        avg = self.engine.get_average_anxiety_by_genre("Pop")
        self.assertIsInstance(avg, float)
        self.assertAlmostEqual(3.0, avg, places=2)

    def test_get_average_anxiety_by_genre_no_matches_returns_none(self) -> None:
        """
        GIVEN a genre that does not exist as favourite in the dataset
        WHEN get_average_anxiety_by_genre is called
        THEN it should return None instead of raising an exception.
        """
        avg = self.engine.get_average_anxiety_by_genre("Country")
        self.assertIsNone(avg)

    def test_get_average_depression_by_genre_lofi(self) -> None:
        """
        GIVEN multiple respondents with 'Lofi' as favourite genre
        WHEN get_average_depression_by_genre('Lofi') is called
        THEN it should return the mean of depression scores for those respondents.
        """
        avg_depression = self.engine.get_average_depression_by_genre("Lofi")
        # Only third respondent fav_genre='Lofi': depression_score=4
        self.assertAlmostEqual(4.0, avg_depression, places=2)

    def test_get_count_by_streaming_service(self) -> None:
        """
        GIVEN respondents using different primary streaming services
        WHEN get_count_by_streaming_service is called
        THEN it should return a dictionary mapping service name to count.

        This method will initially scan the list in O(n), but can later
        be optimised with an internal cache/dictionary in O(1) per query.
        """
        counts = self.engine.get_count_by_streaming_service()
        self.assertEqual(2, counts.get("Spotify"))
        self.assertEqual(1, counts.get("YouTube Music"))
        self.assertEqual(2, len(counts))

    def test_get_users_with_insomnia_score_above_threshold(self) -> None:
        """
        GIVEN a threshold value
        WHEN get_users_with_insomnia_score(threshold) is called
        THEN it should return a list of SurveyResponse objects where
             insomnia_score is strictly greater than the threshold.
        """
        result = self.engine.get_users_with_insomnia_score(threshold=5)
        # Only the second respondent has insomnia_score=8 (>5)
        self.assertEqual(1, len(result))
        self.assertEqual(8, result[0].insomnia_score)

    def test_get_users_with_insomnia_score_boundary_behaviour(self) -> None:
        """
        Edge case: boundary behaviour of the insomnia threshold.

        GIVEN a threshold value t
        WHEN there are users with insomnia_score == t and > t
        THEN define the contract as 'strictly greater than' (> t),
             i.e., equal scores are excluded.
        """
        # Threshold 2; we have insomnia scores [1, 8, 2]
        result = self.engine.get_users_with_insomnia_score(threshold=2)
        self.assertEqual(1, len(result))
        self.assertEqual(8, result[0].insomnia_score)

    def test_get_hours_per_day_vs_average_anxiety(self) -> None:
        """
        GIVEN respondents with varying hours_per_day listening habits
        WHEN get_hours_per_day_vs_average_anxiety is called
        THEN it should return a mapping from hour-range bucket to
             average anxiety score for that bucket.

        Example bucket scheme (to be implemented):
        - "<=1"  for hours_per_day <= 1
        - "1-3"  for 1 < hours_per_day <= 3
        - ">3"   for hours_per_day > 3
        """
        bucket_to_anxiety = self.engine.get_hours_per_day_vs_average_anxiety()

        # With our fixture:
        # - R3: hours_per_day=1.0, anxiety=5 -> bucket "<=1"
        # - R1: hours_per_day=3.0, anxiety=3 -> bucket "1-3"
        # - R2: hours_per_day=5.0, anxiety=7 -> bucket ">3"
        self.assertIn("<=1", bucket_to_anxiety)
        self.assertIn("1-3", bucket_to_anxiety)
        self.assertIn(">3", bucket_to_anxiety)

        self.assertAlmostEqual(5.0, bucket_to_anxiety["<=1"], places=2)
        self.assertAlmostEqual(3.0, bucket_to_anxiety["1-3"], places=2)
        self.assertAlmostEqual(7.0, bucket_to_anxiety[">3"], places=2)


if __name__ == "__main__":
    unittest.main()
