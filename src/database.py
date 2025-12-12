"""
Database access layer for the Music & Mental Health Insights Tool.

This module defines DatabaseManager, which encapsulates all SQLite
interactions. At this stage we provide only minimal stubs so tests
can import the class and drive its implementation.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import List, Tuple

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
        raise NotImplementedError

    def create_tables(self) -> None:
        """
        Create the Respondents and HealthStats tables if they do not exist.

        Stub implementation; tests expect this to be idempotent.
        """
        raise NotImplementedError

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
        raise NotImplementedError

    def get_respondent_count(self) -> int:
        """
        Return the number of rows in the Respondents table.
        """
        raise NotImplementedError

    def get_all_health_stats_joined(self) -> List[Tuple[int, int, int, int, int]]:
        """
        Return a list of rows joining Respondents and HealthStats tables.

        Each row is expected to contain:
        (age, anxiety_score, depression_score, insomnia_score, ocd_score)
        """
        raise NotImplementedError
