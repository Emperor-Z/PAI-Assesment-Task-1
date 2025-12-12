"""Ensure tests do not emit noisy stdout logs."""

import csv
import io
import os
import tempfile
import unittest
from contextlib import redirect_stdout, redirect_stderr

from src.ingestion import load_survey_responses_from_csv  # type: ignore[import]


class TestLoggingCleanliness(unittest.TestCase):
    def test_hours_vs_anxiety_request_has_clean_stdout(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = os.path.join(tmpdir, "noisy.csv")
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
                        "Age": "",
                        "Primary streaming service": "Spotify",
                        "Hours per day": "3.0",
                        "While working": "Yes",
                        "Instrumentalist": "No",
                        "Composer": "No",
                        "Fav genre": "Lofi",
                        "Exploratory": "Yes",
                        "Foreign languages": "No",
                        "BPM": "",
                        "Frequency [Pop]": "Sometimes",
                        "Anxiety": "4",
                        "Depression": "3",
                        "Insomnia": "2",
                        "OCD": "1",
                        "Music effects": "Helps",
                    }
                )

            stdout_buffer = io.StringIO()
            stderr_buffer = io.StringIO()
            with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
                load_survey_responses_from_csv(csv_path, strict=False)
            output = stdout_buffer.getvalue() + stderr_buffer.getvalue()
            self.assertNotIn("Skipping invalid row", output)
