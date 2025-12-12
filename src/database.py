"""
Database access layer for the Music & Mental Health Insights Tool.

This module defines DatabaseManager, which encapsulates all SQLite
interactions. At this stage we provide only minimal stubs so tests
can import the class and drive its implementation.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from typing import List, Optional, Tuple

from src.filters import AGE_GROUP_RULES, FilterCriteria
from src.models import SurveyResponse



@dataclass
class DatabaseManager:
    """
    Manage connections and operations on the SQLite database.

    Parameters
    ----------
    db_path : str
        Path to the SQLite database file. For tests we will use ':memory:'.
    """

    db_path: str
    connection: sqlite3.Connection | None = None

    def connect(self) -> None:
        """
        Establish a connection to the SQLite database.

        Stub implementation for now; real logic will be added to
        satisfy unit tests.
        """
        self.connection = sqlite3.connect(self.db_path)
        self.connection.row_factory = sqlite3.Row

    def create_tables(self) -> None:
        """
        Create the Respondents and HealthStats tables if they do not exist.

        Stub implementation; tests expect this to be idempotent.
        """
        if self.connection is None:
            raise RuntimeError("Database connection not established. Call connect() first.")

        cursor = self.connection.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS Respondents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                age INTEGER NOT NULL,
                primary_streaming_service TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                hours_per_day REAL NOT NULL,
                while_working INTEGER NOT NULL,
                instrumentalist INTEGER NOT NULL,
                composer INTEGER NOT NULL,
                fav_genre TEXT NOT NULL,
                exploratory INTEGER NOT NULL,
                foreign_languages INTEGER NOT NULL,
                bpm INTEGER,
                music_effects TEXT NOT NULL,
                genre_frequencies TEXT NOT NULL
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS HealthStats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                respondent_id INTEGER NOT NULL,
                anxiety INTEGER NOT NULL,
                depression INTEGER NOT NULL,
                insomnia INTEGER NOT NULL,
                ocd INTEGER NOT NULL,
                FOREIGN KEY (respondent_id) REFERENCES Respondents(id)
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS RawResponses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                raw_json TEXT NOT NULL,
                ingestion_error TEXT
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS RejectedRows (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                raw_row_id INTEGER,
                reason TEXT NOT NULL,
                raw_payload TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        self.connection.commit()
        self._ensure_respondent_columns()

    def close(self) -> None:
        """
        Close the database connection if it is open.
        """
        if self.connection is not None:
            self.connection.close()
            self.connection = None

    def insert_survey_response(self, response: SurveyResponse) -> int:
        """
        Insert a SurveyResponse into the Respondents and HealthStats tables.

        Stub implementation for now; will be implemented using
        parameterised queries.
        """
        if self.connection is None:
            raise RuntimeError("Database connection not established. Call connect() first.")

        cursor = self.connection.cursor()
        cursor.execute(
            """
            INSERT INTO Respondents (
                age,
                primary_streaming_service,
                timestamp,
                hours_per_day,
                while_working,
                instrumentalist,
                composer,
                fav_genre,
                exploratory,
                foreign_languages,
                bpm,
                music_effects,
                genre_frequencies
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                response.age,
                response.primary_streaming_service,
                response.timestamp,
                response.hours_per_day,
                int(response.while_working),
                int(response.instrumentalist),
                int(response.composer),
                response.fav_genre,
                int(response.exploratory),
                int(response.foreign_languages),
                response.bpm,
                response.music_effects,
                json.dumps(response.genre_frequencies),
            ),
        )
        respondent_id = cursor.lastrowid
        cursor.execute(
            """
            INSERT INTO HealthStats (respondent_id, anxiety, depression, insomnia, ocd)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                respondent_id,
                response.anxiety_score,
                response.depression_score,
                response.insomnia_score,
                response.ocd_score,
            ),
        )
        self.connection.commit()
        return int(respondent_id)

    def get_respondent_count(self) -> int:
        """
        Return the number of rows in the Respondents table.
        """
        return self._count_rows("Respondents")

    def get_raw_row_count(self) -> int:
        """Return total number of staged raw rows."""
        return self._count_rows("RawResponses")

    def get_clean_row_count(self) -> int:
        """Return total number of curated respondents."""
        return self._count_rows("Respondents")

    def get_rejected_row_count(self) -> int:
        """Return total number of rows captured in RejectedRows."""
        return self._count_rows("RejectedRows")

    def get_top_rejection_reasons(self, limit: int = 10) -> List[Tuple[str, int]]:
        """
        Return the most frequent rejection reasons.

        Parameters
        ----------
        limit:
            Maximum number of reasons to return, must be positive.
        """
        if limit <= 0:
            raise ValueError("limit must be a positive integer")
        if self.connection is None:
            raise RuntimeError("Database connection not established. Call connect() first.")

        cursor = self.connection.cursor()
        cursor.execute(
            """
            SELECT reason, COUNT(*) AS reason_count
            FROM RejectedRows
            GROUP BY reason
            ORDER BY reason_count DESC, reason ASC
            LIMIT ?
            """,
            (limit,),
        )
        rows = cursor.fetchall()
        return [(row["reason"], int(row["reason_count"])) for row in rows]

    def get_all_health_stats_joined(self) -> List[Tuple[int, int, int, int, int]]:
        """
        Return a list of rows joining Respondents and HealthStats tables.

        Each row is expected to contain:
        (age, anxiety_score, depression_score, insomnia_score, ocd_score)
        """
        if self.connection is None:
            raise RuntimeError("Database connection not established. Call connect() first.")

        cursor = self.connection.cursor()
        cursor.execute(
            """
            SELECT
                Respondents.age,
                HealthStats.anxiety,
                HealthStats.depression,
                HealthStats.insomnia,
                HealthStats.ocd
            FROM Respondents
            INNER JOIN HealthStats
                ON Respondents.id = HealthStats.respondent_id
            ORDER BY Respondents.id ASC
            """
        )
        return cursor.fetchall()

    def get_respondent_by_id(self, respondent_id: int) -> Optional[sqlite3.Row]:
        """
        Retrieve a single respondent row by id.
        """
        if self.connection is None:
            raise RuntimeError("Database connection not established. Call connect() first.")

        cursor = self.connection.cursor()
        cursor.execute(
            """
            SELECT id, age, primary_streaming_service as service
            FROM Respondents
            WHERE id = ?
            """,
            (respondent_id,),
        )
        return cursor.fetchone()

    def update_primary_streaming_service(self, respondent_id: int, new_service: str) -> None:
        """
        Update the primary streaming service for a respondent.
        """
        if self.connection is None:
            raise RuntimeError("Database connection not established. Call connect() first.")

        cursor = self.connection.cursor()
        cursor.execute(
            """
            UPDATE Respondents
            SET primary_streaming_service = ?
            WHERE id = ?
            """,
            (new_service, respondent_id),
        )
        self.connection.commit()

    def delete_respondent_and_health_stats(self, respondent_id: int) -> None:
        """
        Delete a respondent and their related health stats entries.
        """
        if self.connection is None:
            raise RuntimeError("Database connection not established. Call connect() first.")

        cursor = self.connection.cursor()
        cursor.execute(
            "DELETE FROM HealthStats WHERE respondent_id = ?",
            (respondent_id,),
        )
        cursor.execute(
            "DELETE FROM Respondents WHERE id = ?",
            (respondent_id,),
        )
        self.connection.commit()

    def insert_raw_response(self, raw_row: dict[str, str], error: str | None) -> int:
        """
        Store a raw CSV row as JSON for auditing.
        """
        if self.connection is None:
            raise RuntimeError("Database connection not established. Call connect() first.")

        cursor = self.connection.cursor()
        cursor.execute(
            """
            INSERT INTO RawResponses (raw_json, ingestion_error)
            VALUES (?, ?)
            """,
            (json.dumps(raw_row), error),
        )
        self.connection.commit()
        return int(cursor.lastrowid)

    def get_all_raw_responses(self) -> List[dict[str, str]]:
        """
        Return all raw responses decoded from JSON.
        """
        if self.connection is None:
            raise RuntimeError("Database connection not established. Call connect() first.")

        cursor = self.connection.cursor()
        cursor.execute("SELECT raw_json FROM RawResponses ORDER BY id ASC")
        rows = cursor.fetchall()
        return [json.loads(row[0]) for row in rows]

    def get_all_clean_responses(self) -> List[SurveyResponse]:
        """
        Return SurveyResponse objects sourced from the curated tables.
        """
        if self.connection is None:
            raise RuntimeError("Database connection not established. Call connect() first.")

        cursor = self.connection.cursor()
        cursor.execute(
            """
            SELECT
                Respondents.timestamp,
                Respondents.age,
                Respondents.primary_streaming_service,
                Respondents.hours_per_day,
                Respondents.while_working,
                Respondents.instrumentalist,
                Respondents.composer,
                Respondents.fav_genre,
                Respondents.exploratory,
                Respondents.foreign_languages,
                Respondents.bpm,
                Respondents.music_effects,
                Respondents.genre_frequencies,
                HealthStats.anxiety,
                HealthStats.depression,
                HealthStats.insomnia,
                HealthStats.ocd
            FROM Respondents
            INNER JOIN HealthStats
                ON Respondents.id = HealthStats.respondent_id
            ORDER BY Respondents.id ASC
            """
        )
        responses: List[SurveyResponse] = []
        for row in cursor.fetchall():
            responses.append(self._row_to_survey_response(row))
        return responses

    def get_distinct_streaming_services(self) -> List[str]:
        """Return sorted list of streaming services present in curated data."""
        if self.connection is None:
            raise RuntimeError("Database connection not established. Call connect() first.")
        cursor = self.connection.cursor()
        cursor.execute(
            """
            SELECT DISTINCT primary_streaming_service
            FROM Respondents
            WHERE primary_streaming_service <> ''
            ORDER BY primary_streaming_service ASC
            """
        )
        return [row[0] for row in cursor.fetchall()]

    def get_distinct_fav_genres(self) -> List[str]:
        """Return sorted list of distinct favourite genres."""
        if self.connection is None:
            raise RuntimeError("Database connection not established. Call connect() first.")
        cursor = self.connection.cursor()
        cursor.execute(
            """
            SELECT DISTINCT fav_genre
            FROM Respondents
            WHERE fav_genre <> ''
            ORDER BY fav_genre ASC
            """
        )
        return [row[0] for row in cursor.fetchall()]

    def get_distinct_music_effects(self) -> List[str]:
        """Return sorted list of distinct music effects labels."""
        if self.connection is None:
            raise RuntimeError("Database connection not established. Call connect() first.")
        cursor = self.connection.cursor()
        cursor.execute(
            """
            SELECT DISTINCT music_effects
            FROM Respondents
            WHERE music_effects <> ''
            ORDER BY music_effects ASC
            """
        )
        return [row[0] for row in cursor.fetchall()]

    def get_clean_responses_filtered(
        self,
        criteria: FilterCriteria,
        limit: int | None = None,
    ) -> List[SurveyResponse]:
        """
        Fetch curated SurveyResponse records applying optional filters.
        """
        if self.connection is None:
            raise RuntimeError("Database connection not established. Call connect() first.")

        if limit is not None:
            if not isinstance(limit, int) or limit <= 0:
                raise ValueError("limit must be a positive integer")

        query = [
            """
            SELECT
                Respondents.timestamp,
                Respondents.age,
                Respondents.primary_streaming_service,
                Respondents.hours_per_day,
                Respondents.while_working,
                Respondents.instrumentalist,
                Respondents.composer,
                Respondents.fav_genre,
                Respondents.exploratory,
                Respondents.foreign_languages,
                Respondents.bpm,
                Respondents.music_effects,
                Respondents.genre_frequencies,
                HealthStats.anxiety,
                HealthStats.depression,
                HealthStats.insomnia,
                HealthStats.ocd
            FROM Respondents
            INNER JOIN HealthStats
                ON Respondents.id = HealthStats.respondent_id
            """
        ]
        where_clauses: List[str] = []
        params: List[object] = []

        if criteria.age_group:
            bounds = AGE_GROUP_RULES.get(criteria.age_group)
            if bounds is not None:
                min_age, max_age = bounds
                if min_age is not None:
                    where_clauses.append("Respondents.age >= ?")
                    params.append(min_age)
                if max_age is not None:
                    where_clauses.append("Respondents.age <= ?")
                    params.append(max_age)

        if criteria.streaming_service:
            where_clauses.append("Respondents.primary_streaming_service = ?")
            params.append(criteria.streaming_service)

        if criteria.favourite_genre:
            where_clauses.append("Respondents.fav_genre = ?")
            params.append(criteria.favourite_genre)

        if criteria.music_effects:
            where_clauses.append("Respondents.music_effects = ?")
            params.append(criteria.music_effects)

        for attr in (
            "while_working",
            "instrumentalist",
            "composer",
            "exploratory",
            "foreign_languages",
        ):
            value = getattr(criteria, attr)
            if value is not None:
                where_clauses.append(f"Respondents.{attr} = ?")
                params.append(int(value))

        if criteria.hours_bucket:
            hours_clauses, hours_params = self._hours_bucket_filters(criteria.hours_bucket)
            where_clauses.extend(hours_clauses)
            params.extend(hours_params)

        if where_clauses:
            query.append("WHERE " + " AND ".join(where_clauses))

        query.append("ORDER BY Respondents.id ASC")

        if limit is not None:
            query.append("LIMIT ?")
            params.append(limit)

        cursor = self.connection.cursor()
        cursor.execute("\n".join(query), params)
        rows = cursor.fetchall()
        return [self._row_to_survey_response(row) for row in rows]

    def _hours_bucket_filters(self, bucket: str) -> tuple[List[str], List[float]]:
        """Translate an hours bucket into SQL clauses and parameters."""
        if bucket == "<=1":
            return (["Respondents.hours_per_day <= ?"], [1.0])
        if bucket == "1-3":
            return (
                [
                    "Respondents.hours_per_day > ?",
                    "Respondents.hours_per_day <= ?",
                ],
                [1.0, 3.0],
            )
        if bucket == ">3":
            return (["Respondents.hours_per_day > ?"], [3.0])
        raise ValueError(f"Unknown hours bucket: {bucket}")

    def _row_to_survey_response(self, row: sqlite3.Row) -> SurveyResponse:
        """Convert a DB row into a SurveyResponse."""
        genre_data = row["genre_frequencies"] or "{}"
        frequencies = json.loads(genre_data)
        return SurveyResponse(
            timestamp=row["timestamp"],
            age=row["age"],
            primary_streaming_service=row["primary_streaming_service"],
            hours_per_day=row["hours_per_day"],
            while_working=bool(row["while_working"]),
            instrumentalist=bool(row["instrumentalist"]),
            composer=bool(row["composer"]),
            fav_genre=row["fav_genre"],
            exploratory=bool(row["exploratory"]),
            foreign_languages=bool(row["foreign_languages"]),
            bpm=row["bpm"],
            anxiety_score=row["anxiety"],
            depression_score=row["depression"],
            insomnia_score=row["insomnia"],
            ocd_score=row["ocd"],
            music_effects=row["music_effects"],
            genre_frequencies=frequencies,
        )

    def _ensure_respondent_columns(self) -> None:
        """Add any missing columns introduced after the initial schema."""
        if self.connection is None:
            return
        columns = {
            "timestamp": "TEXT NOT NULL DEFAULT ''",
            "hours_per_day": "REAL NOT NULL DEFAULT 0",
            "while_working": "INTEGER NOT NULL DEFAULT 0",
            "instrumentalist": "INTEGER NOT NULL DEFAULT 0",
            "composer": "INTEGER NOT NULL DEFAULT 0",
            "fav_genre": "TEXT NOT NULL DEFAULT ''",
            "exploratory": "INTEGER NOT NULL DEFAULT 0",
            "foreign_languages": "INTEGER NOT NULL DEFAULT 0",
            "bpm": "INTEGER",
            "music_effects": "TEXT NOT NULL DEFAULT ''",
            "genre_frequencies": "TEXT NOT NULL DEFAULT '{}'",
        }
        for name, definition in columns.items():
            self._ensure_column_exists("Respondents", name, definition)

    def _ensure_column_exists(self, table: str, column: str, definition: str) -> None:
        """Ensure a specific column exists on a table, adding it if required."""
        if self.connection is None:
            return
        cursor = self.connection.cursor()
        cursor.execute(f"PRAGMA table_info({table})")
        existing = {row[1] for row in cursor.fetchall()}
        if column not in existing:
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
            self.connection.commit()

    def _count_rows(self, table: str) -> int:
        """Return COUNT(*) for a table name that is controlled internally."""
        if self.connection is None:
            raise RuntimeError("Database connection not established. Call connect() first.")
        cursor = self.connection.cursor()
        cursor.execute(f"SELECT COUNT(*) AS row_count FROM {table}")
        result = cursor.fetchone()
        return int(result["row_count"] if result is not None else 0)
