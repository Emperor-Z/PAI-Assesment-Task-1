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
    base_dir = os.path.dirname(os.path.dirname(__file__))
    template_dir = os.path.join(base_dir, "templates")
    app = Flask(__name__, template_folder=template_dir)
    app.config["TESTING"] = testing

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

    def _build_service() -> InsightsService:
        """Build an InsightsService using the configured CSV path."""
        path = app.config["CSV_PATH"]
        responses = load_survey_responses_from_csv(path)
        return InsightsService(responses)

    # --- Route definitions will be added next ---
    @app.route("/", methods=["GET"])
    def home() -> str:  # pragma: no cover (stub)
        return render_template("home.html")

    @app.route("/genre", methods=["GET", "POST"])
    def genre_insights() -> str:
        """
        Show a simple form to request genre-based mental health insights.

        In this step we only implement the GET behaviour; POST will be
        implemented in a later TDD step.
        """
        if request.method == "GET":
            stats: Dict[str, float] | None = None
            return render_template("genre.html", stats=stats)

        genre = request.form.get("genre", "").strip()
        stats = None
        if genre:
            service = _build_service()
            service_stats = service.get_average_anxiety_and_depression_by_genre(genre)
            stats = {
                "genre": service_stats.get("genre", genre),
                "avg_anxiety": service_stats.get("avg_anxiety", 0.0),
                "avg_depression": service_stats.get("avg_depression", 0.0),
            }

        return render_template("genre.html", stats=stats)


    @app.route("/streaming", methods=["GET"])
    def streaming_counts() -> str:
        service = _build_service()
        counts = service.get_streaming_service_counts()
        return render_template("streaming.html", counts=counts)

    @app.route("/hours-vs-anxiety", methods=["GET"])
    def hours_vs_anxiety() -> str:
        service = _build_service()
        buckets = service.get_hours_vs_anxiety()
        return render_template("hours_vs_anxiety.html", buckets=buckets)

    return app
