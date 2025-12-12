"""Regression tests for request-scoped database lifecycle handling."""

from __future__ import annotations

import os
import unittest

from src.app import create_app, get_db_manager  # type: ignore[import]


class TestDatabaseLifecycle(unittest.TestCase):
    """Ensure each request gets a fresh DatabaseManager tied to flask.g."""

    def setUp(self) -> None:
        base_dir = os.path.dirname(os.path.dirname(__file__))
        fixtures_csv = os.path.join(base_dir, "tests", "data", "sample_mxmh.csv")
        logs_dir = os.path.join(base_dir, "logs")
        os.makedirs(logs_dir, exist_ok=True)
        test_log_path = os.path.join(logs_dir, "test_db_lifecycle.log")

        self.app = create_app(
            testing=True,
            csv_path=fixtures_csv,
            log_path=test_log_path,
        )
        self.client = self.app.test_client()

    def test_sequential_requests_do_not_crash(self) -> None:
        """Back-to-back requests should not raise sqlite threading errors."""
        first = self.client.get("/")
        self.assertEqual(200, first.status_code)
        second = self.client.get("/?streaming_service=Spotify")
        self.assertEqual(200, second.status_code)

    def test_db_manager_is_request_scoped_and_closed(self) -> None:
        """The helper should reuse per-request instance and close afterwards."""
        with self.app.test_request_context("/"):
            manager_first = get_db_manager()
            self.assertIsNotNone(manager_first.connection)
            self.assertIs(manager_first, get_db_manager())
            first_conn_id = id(manager_first.connection)

        self.assertIsNone(manager_first.connection)

        with self.app.test_request_context("/again"):
            manager_second = get_db_manager()
            self.assertIsNotNone(manager_second.connection)
            self.assertNotEqual(first_conn_id, id(manager_second.connection))
            self.assertIsNot(manager_first, manager_second)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
