"""
Database access layer for the Music & Mental Health Insights Tool.

This module defines DatabaseManager, which encapsulates all SQLite
interactions. At this stage we provide only minimal stubs so tests
can import the class and drive its implementation.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import List, Optional, Tuple

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
                primary_streaming_service TEXT NOT NULL
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
        self.connection.commit()

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
            INSERT INTO Respondents (age, primary_streaming_service)
            VALUES (?, ?)
            """,
            (response.age, response.primary_streaming_service),
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
        if self.connection is None:
            raise RuntimeError("Database connection not established. Call connect() first.")

        cursor = self.connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM Respondents")
        result = cursor.fetchone()
        return int(result[0] if result is not None else 0)

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
