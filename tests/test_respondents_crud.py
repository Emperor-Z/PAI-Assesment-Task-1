"""CRUD feature tests for respondents management."""

from __future__ import annotations

import os
import tempfile
import unittest

from src.app import create_app


class RespondentCrudTests(unittest.TestCase):
    """Ensure browse and CRUD routes function end-to-end."""

    def setUp(self) -> None:
        base_dir = os.path.dirname(os.path.dirname(__file__))
        fixtures_csv = os.path.join(base_dir, "tests", "data", "sample_mxmh.csv")
        self.tmpdir = tempfile.TemporaryDirectory()
        db_path = os.path.join(self.tmpdir.name, "app.db")
        self.app = create_app(testing=True, csv_path=fixtures_csv, db_path=db_path)
        self.client = self.app.test_client()

    def tearDown(self) -> None:
        self.tmpdir.cleanup()

    def test_respondents_list_page_shows_table(self) -> None:
        response = self.client.get("/respondents")
        self.assertEqual(200, response.status_code)
        html = response.data.decode("utf-8")
        self.assertIn("Respondents", html)
        self.assertIn("Streaming service", html)

    def test_create_edit_delete_respondent_flow(self) -> None:
        create_payload = {
            "age": "34",
            "streaming_service": "Spotify",
            "hours_per_day": "3",
            "fav_genre": "Jazz",
            "music_effects": "Helps focus",
            "hours_bucket": "1-3",
            "while_working": "yes",
            "instrumentalist": "no",
            "composer": "no",
            "exploratory": "yes",
            "foreign_languages": "no",
            "anxiety_score": "4",
            "depression_score": "2",
            "insomnia_score": "3",
            "ocd_score": "1",
        }
        response = self.client.post("/respondents/new", data=create_payload, follow_redirects=False)
        self.assertEqual(302, response.status_code)
        detail_url = response.headers["Location"]
        detail = self.client.get(detail_url)
        self.assertEqual(200, detail.status_code)
        html = detail.data.decode("utf-8")
        self.assertIn("Jazz", html)
        respondent_id = detail_url.rstrip("/").split("/")[-1]

        update_payload = dict(create_payload)
        update_payload["fav_genre"] = "Ambient"
        response = self.client.post(
            f"/respondents/{respondent_id}/edit",
            data=update_payload,
            follow_redirects=True,
        )
        self.assertEqual(200, response.status_code)
        self.assertIn("Ambient", response.data.decode("utf-8"))

        response = self.client.post(f"/respondents/{respondent_id}/delete", follow_redirects=True)
        self.assertEqual(200, response.status_code)
        self.assertNotIn("Ambient", response.data.decode("utf-8"))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
