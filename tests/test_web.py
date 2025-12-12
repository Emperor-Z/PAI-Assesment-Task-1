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
        self.assertIn("/static/css/style.css", html)

    def test_home_page_includes_filter_form(self) -> None:
        """Home page should expose filter dropdowns for analytics."""
        response = self.client.get("/")
        self.assertEqual(200, response.status_code)
        html = response.data.decode("utf-8")
        self.assertIn('name="streaming_service"', html)
        self.assertIn('name="age_group"', html)
        self.assertIn("Total cleaned respondents", html)

    def test_home_page_filters_adjust_counts(self) -> None:
        """Applying filters should change overview totals."""
        baseline = self.client.get("/")
        filtered = self.client.get("/?streaming_service=Spotify")
        self.assertIn("Total cleaned respondents: 2", baseline.data.decode("utf-8"))
        self.assertIn("Total cleaned respondents: 1", filtered.data.decode("utf-8"))

    def test_home_page_shows_cleaning_kpis(self) -> None:
        """Home dashboard should expose data quality KPI labels."""
        response = self.client.get("/")
        self.assertEqual(200, response.status_code)
        html = response.data.decode("utf-8")
        self.assertIn("Raw rows", html)
        self.assertIn("Cleaned rows", html)
        self.assertIn("Rejected rows", html)
        self.assertIn("Top rejection reasons", html)

    def test_home_page_shows_extended_health_kpis(self) -> None:
        """Dashboard should highlight broader health KPIs and context note."""
        response = self.client.get("/")
        self.assertEqual(200, response.status_code)
        html = response.data.decode("utf-8")
        expected_labels = [
            "Top streaming service",
            "Top favourite genre",
            "Average insomnia",
            "Average OCD",
            "Rejected rows",
        ]
        for label in expected_labels:
            self.assertIn(label, html)
        self.assertIn("Insights are associative", html)

    def test_home_page_includes_chart_placeholders(self) -> None:
        """Home should embed the main suite of health impact visuals."""
        response = self.client.get("/")
        self.assertEqual(200, response.status_code)
        html = response.data.decode("utf-8")
        chart_paths = [
            "/charts/streaming.png",
            "/charts/hours-vs-anxiety.png",
            "/charts/anxiety-distribution.png",
            "/charts/age-group-means.png",
            "/charts/music-effects.png",
            "/charts/top-genres.png",
        ]
        for path in chart_paths:
            self.assertIn(path, html)

    def test_data_quality_page_renders_counts_and_reasons(self) -> None:
        """Dedicated data quality view should show counts and rejection reasons."""
        response = self.client.get("/data-quality")
        self.assertEqual(200, response.status_code)
        html = response.data.decode("utf-8")
        self.assertIn("Data quality overview", html)
        self.assertIn("Raw rows", html)
        self.assertIn("Top rejection reasons", html)
        self.assertIn("Cleaning rules", html)

    def test_rejected_rows_export_returns_csv(self) -> None:
        response = self.client.get("/export/rejected.csv")
        self.assertEqual(200, response.status_code)
        self.assertEqual("text/csv; charset=utf-8", response.headers.get("Content-Type"))
        self.assertIn("reason", response.data.decode("utf-8").splitlines()[0])

    def test_export_page_renders_controls(self) -> None:
        response = self.client.get("/export")
        self.assertEqual(200, response.status_code)
        html = response.data.decode("utf-8")
        self.assertIn("Select dataset", html)
        self.assertIn("Columns", html)
        self.assertIn("Row limit", html)

    def test_export_download_respects_columns_and_filters(self) -> None:
        response = self.client.get(
            "/export/download.csv?dataset=clean&columns=age&columns=primary_streaming_service&limit=1&streaming_service=Spotify"
        )
        self.assertEqual(200, response.status_code)
        self.assertEqual("text/csv; charset=utf-8", response.headers.get("Content-Type"))
        lines = response.data.decode("utf-8").strip().splitlines()
        self.assertEqual("age,primary_streaming_service", lines[0].lower())
        self.assertEqual(2, len(lines))
        self.assertIn("Spotify", lines[1])

    def test_export_download_all_columns_defaults(self) -> None:
        response = self.client.get("/export/download.csv?dataset=clean")
        self.assertEqual(200, response.status_code)
        header = response.data.decode("utf-8").splitlines()[0].lower()
        self.assertIn("age", header)
        self.assertIn("primary_streaming_service", header)

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
        GIVEN a dataset with known genres
        WHEN posting to the genre insights route
        THEN the response should include respondent counts and means.
        """
        import csv
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = os.path.join(tmpdir, "genres.csv")
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
            test_app = create_app(testing=True, csv_path=csv_path, db_path=":memory:")
            client = test_app.test_client()
            response = client.post(
                "/genre",
                data={"genre": "Lofi", "metric": "anxiety", "min_n": "1"},
                follow_redirects=True,
            )
            self.assertEqual(200, response.status_code)
            html = response.data.decode("utf-8")
            self.assertIn("Lofi", html)
            self.assertIn("Respondents:", html)
            self.assertIn("Anxiety mean", html)

    def test_genre_page_warns_when_min_n_high(self) -> None:
        import csv
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = os.path.join(tmpdir, "genres.csv")
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
            test_app = create_app(testing=True, csv_path=csv_path, db_path=":memory:")
            client = test_app.test_client()
            response = client.get("/genre?genre=Lofi&min_n=5")
            self.assertEqual(200, response.status_code)
            html = response.data.decode("utf-8")
            self.assertIn("min_n=5", html)

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

    def test_anxiety_distribution_chart_endpoint_returns_png(self) -> None:
        response = self.client.get("/charts/anxiety-distribution.png")
        self.assertEqual(200, response.status_code)
        self.assertIn("image/png", response.headers.get("Content-Type", ""))
        self.assertGreater(len(response.data), 100)

    def test_age_group_means_chart_endpoint_returns_png(self) -> None:
        response = self.client.get("/charts/age-group-means.png")
        self.assertEqual(200, response.status_code)
        self.assertIn("image/png", response.headers.get("Content-Type", ""))
        self.assertGreater(len(response.data), 100)

    def test_music_effects_chart_endpoint_returns_png(self) -> None:
        response = self.client.get("/charts/music-effects.png")
        self.assertEqual(200, response.status_code)
        self.assertIn("image/png", response.headers.get("Content-Type", ""))
        self.assertGreater(len(response.data), 100)

    def test_top_genres_chart_endpoint_returns_png(self) -> None:
        response = self.client.get("/charts/top-genres.png")
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

    def test_configurable_export_download(self) -> None:
        """Export endpoint should honour columns and limits."""
        response = self.client.get("/export/download.csv?dataset=clean&columns=age&columns=anxiety_score&limit=1")
        self.assertEqual(200, response.status_code)
        self.assertIn("text/csv", response.headers.get("Content-Type", ""))
        body = response.data.decode("utf-8").strip().splitlines()
        self.assertEqual("age,anxiety_score", body[0])
        self.assertEqual(2, len(body))  # header + one row

    def test_configurable_export_rejects_invalid_columns(self) -> None:
        """Unknown columns should trigger a 400."""
        response = self.client.get("/export/download.csv?dataset=clean&columns=unknown_column")
        self.assertEqual(400, response.status_code)

    def test_export_page_renders_form(self) -> None:
        """Export configuration page should include filter inputs."""
        response = self.client.get("/export")
        self.assertEqual(200, response.status_code)
        html = response.data.decode("utf-8")
        self.assertIn("Select dataset", html)
        self.assertIn("Row limit", html)
