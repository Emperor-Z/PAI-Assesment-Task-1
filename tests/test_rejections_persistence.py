"""Tests ensuring invalid rows are persisted in the RejectedRows table."""

from __future__ import annotations

import csv
import os
import tempfile
import unittest
from typing import List

from src.database import DatabaseManager
from src.etl_clean import clean_raw_responses_into_database
from src.etl_stage import ingest_csv_into_raw_database


class TestRejectedRowPersistence(unittest.TestCase):
    """Verify tolerant ETL pipeline writes rejected payloads for auditing."""

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.csv_path = os.path.join(self.temp_dir.name, "mixed_rows.csv")
        self._write_csv_fixture()

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _write_csv_fixture(self) -> None:
        fieldnames: List[str] = [
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
        with open(self.csv_path, "w", newline="", encoding="utf-8") as csv_file:
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

    def test_rejected_rows_recorded_with_reasons(self) -> None:
        """Invalid rows should populate RejectedRows with meaningful reasons."""
        db_manager = DatabaseManager(":memory:")
        db_manager.connect()
        db_manager.create_tables()

        ingest_csv_into_raw_database(self.csv_path, db_manager)
        clean_raw_responses_into_database(db_manager)

        self.assertEqual(1, db_manager.get_rejected_row_count())
        reasons = db_manager.get_top_rejection_reasons(limit=5)
        self.assertIn(("Age is required but was empty.", 1), reasons)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
