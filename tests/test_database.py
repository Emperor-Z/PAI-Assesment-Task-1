"""
Unit tests for the database layer (DatabaseManager).

These tests are written BEFORE implementing the actual database logic,
following the TDD process:

    1. Write tests (Red)
    2. Implement minimal code to pass tests (Green)
    3. Refactor while keeping tests green

The tests use an in-memory SQLite database (':memory:') so they do not
create any real files on disk and can run quickly and repeatedly.
"""

import json
import sqlite3
import unittest
from typing import Dict

from src.database import DatabaseManager  # type: ignore[import]
from src.filters import FilterCriteria
from src.models import SurveyResponse  # type: ignore[import]


class TestDatabaseManager(unittest.TestCase):
    """
    Tests for DatabaseManager, which encapsulates all SQLite interactions.

    These tests verify:
    - tables are created correctly,
    - inserts from SurveyResponse work as expected,
    - simple queries and joins behave correctly.
    """

    def setUp(self) -> None:
        """
        Create a new DatabaseManager with an in-memory SQLite database
        before each test.

        Using ':memory:' ensures each test has a clean, isolated database.
        """
        self.db_manager = DatabaseManager(":memory:")
        self.db_manager.connect()
        self.db_manager.create_tables()

    def tearDown(self) -> None:
        """
        Close the database connection after each test to release resources.
        """
        self.db_manager.close()

    def _make_sample_response(self) -> SurveyResponse:
        """
        Helper to create a minimal, valid SurveyResponse for insertion.
        """
        genre_frequencies: Dict[str, int] = {
            "Lofi": 3,
            "Pop": 2,
        }

        return SurveyResponse(
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

    def test_create_tables_idempotent(self) -> None:
        """
        GIVEN a freshly connected DatabaseManager
        WHEN create_tables is called multiple times
        THEN it should not raise errors (idempotent behaviour).

        This ensures CREATE TABLE statements use IF NOT EXISTS.
        """
        # First call is in setUp
        self.db_manager.create_tables()  # second call
        self.db_manager.create_tables()  # third call
        # If we reach this point without sqlite3.Error, the test passes.

    def test_insert_survey_response_and_count_rows(self) -> None:
        """
        GIVEN a valid SurveyResponse
        WHEN insert_survey_response is called
        THEN one row should appear in Respondents and HealthStats tables,
             and get_respondent_count should return 1.
        """
        response = self._make_sample_response()
        respondent_id = self.db_manager.insert_survey_response(response)

        # Basic type check on returned id
        self.assertIsInstance(respondent_id, int)

        # Check respondent count via helper method
        count = self.db_manager.get_respondent_count()
        self.assertEqual(1, count)

        # Also verify directly via a SQL query
        cursor = self.db_manager.connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM Respondents")
        respondents_rows = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM HealthStats")
        health_rows = cursor.fetchone()[0]

        self.assertEqual(1, respondents_rows)
        self.assertEqual(1, health_rows)

    def test_create_raw_responses_table(self) -> None:
        """
        GIVEN create_tables has been run
        WHEN introspecting RawResponses
        THEN the expected columns should exist.
        """
        cursor = self.db_manager.connection.cursor()
        cursor.execute("PRAGMA table_info(RawResponses)")
        columns = cursor.fetchall()
        column_names = {col[1] for col in columns}
        self.assertIn("id", column_names)
        self.assertIn("raw_json", column_names)
        self.assertIn("ingestion_error", column_names)

    def test_health_stats_join_query(self) -> None:
        """
        GIVEN at least one inserted SurveyResponse
        WHEN get_all_health_stats_joined is called
        THEN it should return rows where Respondents and HealthStats are joined.

        We expect at least one row with (age, anxiety, depression, insomnia, ocd).
        """
        response = self._make_sample_response()
        self.db_manager.insert_survey_response(response)

        rows = self.db_manager.get_all_health_stats_joined()
        self.assertGreaterEqual(len(rows), 1)

        # Unpack first row
        age, anxiety, depression, insomnia, ocd = rows[0]

        self.assertEqual(21, age)
        self.assertEqual(5, anxiety)
        self.assertEqual(4, depression)
        self.assertEqual(6, insomnia)
        self.assertEqual(2, ocd)

    def test_parameterised_queries_used_for_insert(self) -> None:
        """
        GIVEN a SurveyResponse containing a 'strange' streaming service string
        WHEN insert_survey_response is called
        THEN it should not crash or allow SQL injection.

        This test is more about design: we verify that insert_survey_response
        uses parameterised queries internally by:
        - passing a string that *would* break raw string interpolation, and
        - asserting that no sqlite3.Error is raised.
        """
        malicious_service = "Spotify'); DROP TABLE Respondents; --"
        response = self._make_sample_response()
        response.primary_streaming_service = malicious_service

        try:
            self.db_manager.insert_survey_response(response)
        except sqlite3.Error as exc:
            self.fail(f"insert_survey_response raised sqlite3.Error: {exc!r}")

    def test_insert_raw_response(self) -> None:
        """
        GIVEN a raw row dict
        WHEN insert_raw_response is called
        THEN the row should be stored as JSON in RawResponses.
        """
        raw_row = {"Age": "21", "Primary streaming service": "Spotify"}
        raw_id = self.db_manager.insert_raw_response(raw_row, error=None)

        cursor = self.db_manager.connection.cursor()
        cursor.execute("SELECT raw_json, ingestion_error FROM RawResponses WHERE id = ?", (raw_id,))
        stored_row = cursor.fetchone()
        self.assertIsNotNone(stored_row)
        self.assertIsNone(stored_row[1])
        decoded = json.loads(stored_row[0])
        self.assertEqual(raw_row, decoded)

    def test_get_all_raw_responses_returns_decoded_rows(self) -> None:
        """
        Raw rows should be returned as dictionaries for the cleaning step.
        """
        raw_row = {"Age": "21", "Primary streaming service": "Spotify"}
        second_row = {"Age": "", "Primary streaming service": "YouTube"}
        self.db_manager.insert_raw_response(raw_row, error=None)
        self.db_manager.insert_raw_response(second_row, error="bad age")

        rows = self.db_manager.get_all_raw_responses()
        self.assertEqual(2, len(rows))
        self.assertIn(raw_row, rows)

    def test_get_all_clean_responses_returns_survey_responses(self) -> None:
        """
        Clean responses should be fetched as SurveyResponse objects.
        """
        response = self._make_sample_response()
        self.db_manager.insert_survey_response(response)

        responses = self.db_manager.get_all_clean_responses()
        self.assertEqual(1, len(responses))
        clean = responses[0]
        self.assertEqual(response.age, clean.age)
        self.assertEqual(response.primary_streaming_service, clean.primary_streaming_service)
        self.assertEqual(response.anxiety_score, clean.anxiety_score)

    def test_get_respondent_by_id_returns_correct_row(self) -> None:
        """
        GIVEN an inserted SurveyResponse
        WHEN get_respondent_by_id is called
        THEN it should return the correct respondent row including age and service.
        """
        response = self._make_sample_response()
        respondent_id = self.db_manager.insert_survey_response(response)

        row = self.db_manager.get_respondent_by_id(respondent_id)
        self.assertIsNotNone(row)
        self.assertEqual(respondent_id, row["id"])
        self.assertEqual(response.age, row["age"])
        self.assertEqual(response.primary_streaming_service, row["service"])

    def test_update_primary_streaming_service_changes_value(self) -> None:
        """
        GIVEN an inserted respondent
        WHEN update_primary_streaming_service is called
        THEN the Respondents.service column should be updated.
        """
        response = self._make_sample_response()
        respondent_id = self.db_manager.insert_survey_response(response)

        self.db_manager.update_primary_streaming_service(respondent_id, "UpdatedService")

        row = self.db_manager.get_respondent_by_id(respondent_id)
        self.assertIsNotNone(row)
        self.assertEqual("UpdatedService", row["service"])

    def test_delete_respondent_and_health_stats_removes_rows(self) -> None:
        """
        GIVEN an inserted respondent with health stats
        WHEN delete_respondent_and_health_stats is called
        THEN both Respondents and HealthStats rows should be removed.
        """
        response = self._make_sample_response()
        respondent_id = self.db_manager.insert_survey_response(response)

        self.db_manager.delete_respondent_and_health_stats(respondent_id)

        row = self.db_manager.get_respondent_by_id(respondent_id)
        self.assertIsNone(row)

        joined = self.db_manager.get_all_health_stats_joined()
        self.assertEqual(0, len(joined))

    def test_get_filtered_clean_responses(self) -> None:
        """
        GIVEN multiple curated respondents
        WHEN filters are applied
        THEN only matching SurveyResponse objects should be returned.
        """
        teen = self._make_sample_response()
        teen.age = 17
        teen.primary_streaming_service = "Spotify"
        teen.hours_per_day = 1.0
        teen.while_working = True
        teen.music_effects = "Improve"
        self.db_manager.insert_survey_response(teen)

        adult = self._make_sample_response()
        adult.age = 28
        adult.primary_streaming_service = "Apple Music"
        adult.hours_per_day = 4.2
        adult.while_working = False
        adult.music_effects = "Improve"
        adult.fav_genre = "Rock"
        adult.exploratory = False
        adult.foreign_languages = False
        self.db_manager.insert_survey_response(adult)

        older = self._make_sample_response()
        older.age = 46
        older.primary_streaming_service = "Spotify"
        older.hours_per_day = 2.0
        older.while_working = False
        older.music_effects = "No effect"
        older.fav_genre = "Classical"
        self.db_manager.insert_survey_response(older)

        criteria = FilterCriteria(
            age_group="25-34",
            streaming_service="Apple Music",
            favourite_genre="Rock",
            music_effects="Improve",
            while_working=False,
            hours_bucket=">3",
        )

        filtered = self.db_manager.get_clean_responses_filtered(criteria)
        self.assertEqual(1, len(filtered))
        self.assertEqual("Apple Music", filtered[0].primary_streaming_service)
        self.assertEqual(4.2, filtered[0].hours_per_day)

        spotify_only = FilterCriteria(streaming_service="Spotify")
        limited = self.db_manager.get_clean_responses_filtered(spotify_only, limit=1)
        self.assertEqual(1, len(limited))

    def test_get_distinct_streaming_services(self) -> None:
        """Distinct streaming services should be sorted and unique."""
        first = self._make_sample_response()
        first.primary_streaming_service = "Spotify"
        second = self._make_sample_response()
        second.primary_streaming_service = "Apple Music"
        third = self._make_sample_response()
        third.primary_streaming_service = "Spotify"
        self.db_manager.insert_survey_response(first)
        self.db_manager.insert_survey_response(second)
        self.db_manager.insert_survey_response(third)

        services = self.db_manager.get_distinct_streaming_services()
        self.assertEqual(["Apple Music", "Spotify"], services)
