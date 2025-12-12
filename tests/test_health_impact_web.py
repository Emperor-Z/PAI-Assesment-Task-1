"""Web tests for health impact routes and charts."""

from __future__ import annotations

import os
import unittest

from src.app import create_app


class TestHealthImpactWeb(unittest.TestCase):
    """Ensure health impact page and charts render."""

    def setUp(self) -> None:
        base_dir = os.path.dirname(os.path.dirname(__file__))
        fixtures_csv = os.path.join(base_dir, "tests", "data", "sample_mxmh.csv")
        logs_dir = os.path.join(base_dir, "logs")
        os.makedirs(logs_dir, exist_ok=True)
        test_log_path = os.path.join(logs_dir, "test_health_impact_web.log")
        self.app = create_app(
            testing=True,
            csv_path=fixtures_csv,
            log_path=test_log_path,
        )
        self.client = self.app.test_client()

    def test_health_impact_page_renders(self) -> None:
        response = self.client.get("/health-impact")
        self.assertEqual(200, response.status_code)
        self.assertIn(b"Health impact", response.data)

    def test_health_impact_chart_endpoints(self) -> None:
        endpoints = [
            "/charts/genre-vs-anxiety.png",
            "/charts/effects-vs-anxiety.png",
            "/charts/hours-vs-scores.png",
        ]
        for endpoint in endpoints:
            resp = self.client.get(endpoint)
            self.assertEqual(200, resp.status_code, endpoint)
            self.assertEqual("image/png", resp.headers.get("Content-Type"))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
