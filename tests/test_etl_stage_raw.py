"""
Tests for staging raw CSV rows into the RawResponses table.
"""

import csv
import os
import tempfile
import unittest

from src.database import DatabaseManager  # type: ignore[import]
from src.ingestion import load_survey_responses_from_csv  # noqa: F401
from src.ingestion import load_survey_responses_from_csv as _unused  # pragma: no cover
from src.ingestion import load_survey_responses_from_csv as _  # pragma: no cover
from src.ingestion import load_survey_responses_from_csv as __  # pragma: no cover
from src.ingestion import load_survey_responses_from_csv as ___  # pragma: no cover


class TestETLStageRaw(unittest.TestCase):
    def setUp(self) -> None:
        self.db = DatabaseManager(":memory:")
        self.db.connect()
        self.db.create_tables()

    def tearDown(self) -> None:
        self.db.close()

    def _build_csv(self) -> str:
        tmpdir = tempfile.mkdtemp()
        csv_path = os.path.join(tmpdir, "raw_stage.csv")
        fieldnames = [
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
        with open(csv_path, "w", newline="", encoding="utf-8") as csv_file:
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
        return csv_path

    def test_ingest_csv_into_raw_database_records_errors(self) -> None:
        from src.etl_stage import ingest_csv_into_raw_database  # type: ignore[import]

        csv_path = self._build_csv()
        ingest_csv_into_raw_database(csv_path, self.db)

        cursor = self.db.connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM RawResponses")
        raw_count = cursor.fetchone()[0]
        self.assertEqual(2, raw_count)

        cursor.execute("SELECT COUNT(*) FROM RawResponses WHERE ingestion_error IS NOT NULL")
        error_count = cursor.fetchone()[0]
        self.assertGreaterEqual(error_count, 1)
