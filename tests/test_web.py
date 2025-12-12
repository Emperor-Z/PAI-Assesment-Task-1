"""
Web layer tests for the Music & Mental Health Insights Tool.

These tests use Flask's test client to verify that:
- routes exist and return HTTP 200,
- the expected content is present in the HTML,
- basic form submission and analytics behaviour works.
"""

import os
import unittest

from src.app import create_app  # type: ignore[import]


class TestWebApp(unittest.TestCase):
    def setUp(self) -> None:
        """
        Create a Flask app instance configured for testing, using the
        small sample CSV and a dedicated test log file.
        """
        base_dir = os.path.dirname(os.path.dirname(__file__))
        fixtures_csv = os.path.join(base_dir, "tests", "data", "sample_mxmh.csv")
        logs_dir = os.path.join(base_dir, "logs")
        os.makedirs(logs_dir, exist_ok=True)
        test_log_path = os.path.join(logs_dir, "test_web.log")

        self.app = create_app(
            testing=True,
            csv_path=fixtures_csv,
            log_path=test_log_path,
        )
        self.client = self.app.test_client()

    def test_home_page_renders_successfully(self) -> None:
        """
        GIVEN the web app
        WHEN requesting the home page
        THEN status code should be 200 and page should mention the tool.
        """
        response = self.client.get("/")
        self.assertEqual(200, response.status_code)
        html = response.data.decode("utf-8")
        self.assertIn("Music & Mental Health Insights", html)

    def test_genre_page_shows_form(self) -> None:
        """
        GIVEN the web app
        WHEN requesting the genre insights page
        THEN the page should include a form to select a genre.
        """
        response = self.client.get("/genre")
        self.assertEqual(200, response.status_code)
        html = response.data.decode("utf-8")
        self.assertIn("<form", html)
        self.assertIn("Select a genre", html)

    def test_genre_form_submission_returns_stats(self) -> None:
        """
        GIVEN a known dataset with 'Lofi' as a favourite genre
        WHEN posting 'Lofi' to the genre insights route
        THEN the response should include that genre and show average values.
        """
        response = self.client.post(
            "/genre",
            data={"genre": "Lofi"},
            follow_redirects=True,
        )
        self.assertEqual(200, response.status_code)
        html = response.data.decode("utf-8")
        self.assertIn("Lofi", html)
        self.assertIn("Average anxiety", html)
        self.assertIn("Average depression", html)

    def test_streaming_counts_page_renders_table(self) -> None:
        """
        GIVEN the web app
        WHEN requesting the streaming service counts page
        THEN it should show a table of streaming service usage.
        """
        response = self.client.get("/streaming")
        self.assertEqual(200, response.status_code)
        html = response.data.decode("utf-8")
        self.assertIn("Streaming service usage", html)
        self.assertIn("<table", html)

    def test_hours_vs_anxiety_page_renders(self) -> None:
        """
        GIVEN the web app
        WHEN requesting the hours-vs-anxiety page
        THEN it should show at least the bucket labels.
        """
        response = self.client.get("/hours-vs-anxiety")
        self.assertEqual(200, response.status_code)
        html = response.data.decode("utf-8")
        self.assertIn("Listening duration vs average anxiety", html)
        self.assertIn("<table", html)

    def test_hours_vs_anxiety_route_handles_dirty_csv(self) -> None:
        """
        GIVEN a CSV with dirty rows
        WHEN building the app and hitting /hours-vs-anxiety
        THEN the response should still be 200 with expected content.
        """
        import csv
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = os.path.join(tmpdir, "mixed.csv")
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

            test_app = create_app(testing=True, csv_path=csv_path, db_path=":memory:")
            client = test_app.test_client()
            response = client.get("/hours-vs-anxiety")
            self.assertEqual(200, response.status_code)
            html = response.data.decode("utf-8")
            self.assertIn("Listening duration vs average anxiety", html)

    def test_streaming_chart_endpoint_returns_png(self) -> None:
        response = self.client.get("/charts/streaming.png")
        self.assertEqual(200, response.status_code)
        self.assertIn("image/png", response.headers.get("Content-Type", ""))
        self.assertGreater(len(response.data), 100)

    def test_hours_vs_anxiety_chart_endpoint_returns_png(self) -> None:
        response = self.client.get("/charts/hours-vs-anxiety.png")
        self.assertEqual(200, response.status_code)
        self.assertIn("image/png", response.headers.get("Content-Type", ""))
        self.assertGreater(len(response.data), 100)

    def test_export_streaming_counts_as_csv(self) -> None:
        """
        GIVEN the web app
        WHEN requesting the streaming counts export endpoint
        THEN a CSV file should be returned with the expected header.
        """
        response = self.client.get("/export/streaming-csv")
        self.assertEqual(200, response.status_code)

        content_type = response.headers.get("Content-Type", "")
        self.assertIn("text/csv", content_type)

        body = response.data.decode("utf-8")
        self.assertIn("service,count", body)
