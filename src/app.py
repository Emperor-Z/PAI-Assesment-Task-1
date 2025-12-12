"""
Flask web application for the Music & Mental Health Insights Tool.

Exposes a simple HTML interface over the InsightsService.
"""

from __future__ import annotations

import os
from typing import Any, Dict

from flask import Flask, render_template, request

from src.ingestion import load_survey_responses_from_csv
from src.logging_utils import configure_logger
from src.services import InsightsService


def create_app(
    testing: bool = False,
    csv_path: str | None = None,
    log_path: str | None = None,
) -> Flask:
    """
    Application factory for the Flask web app.

    Parameters
    ----------
    testing : bool
        If True, configure the app for testing (e.g. no debug toolbar).
    csv_path : str | None
        Optional explicit path to the CSV file. If None, defaults to the
        real dataset under data/mxmh_survey_results.csv.
    log_path : str | None
        Optional explicit path to the log file. If None, defaults to
        logs/app.log.
    """
    app = Flask(__name__)
    app.config["TESTING"] = testing

    base_dir = os.path.dirname(os.path.dirname(__file__))

    if csv_path is None:
        csv_path = os.path.join(base_dir, "data", "mxmh_survey_results.csv")

    if log_path is None:
        logs_dir = os.path.join(base_dir, "logs")
        os.makedirs(logs_dir, exist_ok=True)
        log_path = os.path.join(logs_dir, "app.log")

    # Configure logger for the whole app
    configure_logger(log_path)

    # Store paths in config so route handlers can access them
    app.config["CSV_PATH"] = csv_path
    app.config["LOG_PATH"] = log_path

    # --- Route definitions will be added next ---
    @app.route("/", methods=["GET"])
    def home() -> str:  # pragma: no cover (stub)
        # Placeholder implementation to satisfy imports; tests will drive details.
        return "HOME PLACEHOLDER"

    @app.route("/genre", methods=["GET", "POST"])
    def genre_insights() -> str:  # pragma: no cover (stub)
        return "GENRE PLACEHOLDER"

    @app.route("/streaming", methods=["GET"])
    def streaming_counts() -> str:  # pragma: no cover (stub)
        return "STREAMING PLACEHOLDER"

    @app.route("/hours-vs-anxiety", methods=["GET"])
    def hours_vs_anxiety() -> str:  # pragma: no cover (stub)
        return "HOURS PLACEHOLDER"

    return app
