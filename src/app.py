"""
Flask web application for the Music & Mental Health Insights Tool.

Exposes a simple HTML interface over the InsightsService.
"""

from __future__ import annotations

import os
from typing import Any, Dict

from flask import Flask, Response, make_response, render_template, request

from src.charts import (
    render_hours_vs_anxiety_png,
    render_age_group_means_chart,
    render_music_effects_chart,
    render_score_distribution_chart,
    render_streaming_counts_png,
)
from src.database import DatabaseManager
from src.etl_clean import clean_raw_responses_into_database
from src.etl_stage import ingest_csv_into_raw_database
from src.filters import FilterCriteria
from src.logging_utils import configure_logger
from src.services import InsightsService


def create_app(
    testing: bool = False,
    csv_path: str | None = None,
    log_path: str | None = None,
    db_path: str | None = None,
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

    if db_path is None:
        data_dir = os.path.join(base_dir, "data")
        os.makedirs(data_dir, exist_ok=True)
        db_path = os.path.join(data_dir, "app.db")

    # Configure logger for the whole app
    configure_logger(log_path)

    # Store paths in config so route handlers can access them
    app.config["CSV_PATH"] = csv_path
    app.config["LOG_PATH"] = log_path
    app.config["DB_PATH"] = db_path

    cached_service: InsightsService | None = None
    cached_db_manager: DatabaseManager | None = None

    def _build_service() -> InsightsService:
        """Build an InsightsService using the configured CSV path."""
        nonlocal cached_service, cached_db_manager
        if cached_service is not None:
            return cached_service

        path = app.config["CSV_PATH"]
        database_path = app.config["DB_PATH"]
        db_manager = DatabaseManager(database_path)
        db_manager.connect()
        db_manager.create_tables()

        if db_manager.get_respondent_count() == 0:
            ingest_csv_into_raw_database(path, db_manager)
            clean_raw_responses_into_database(db_manager)

        cached_db_manager = db_manager
        cached_service = InsightsService(db_manager)
        return cached_service

    def _parse_filter_criteria() -> FilterCriteria:
        """Build filter criteria from query parameters."""
        return FilterCriteria.from_request_args(request.args.to_dict(flat=True))

    # --- Route definitions will be added next ---
    @app.route("/", methods=["GET"])
    def home() -> str:
        service = _build_service()
        counts = service.get_streaming_service_counts()
        total_respondents = sum(counts.values())
        top_service = max(counts, key=counts.get) if counts else "N/A"
        return render_template(
            "home.html",
            total_respondents=total_respondents,
            top_service=top_service,
        )

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

    @app.route("/charts/streaming.png", methods=["GET"])
    def streaming_chart() -> Response:
        service = _build_service()
        counts = service.get_streaming_service_counts()
        png = render_streaming_counts_png(counts)
        return Response(png, mimetype="image/png")

    @app.route("/charts/hours-vs-anxiety.png", methods=["GET"])
    def hours_vs_anxiety_chart() -> Response:
        service = _build_service()
        buckets = service.get_hours_vs_anxiety()
        png = render_hours_vs_anxiety_png(buckets)
        return Response(png, mimetype="image/png")

    @app.route("/charts/anxiety-distribution.png", methods=["GET"])
    def anxiety_distribution_chart() -> Response:
        service = _build_service()
        criteria = _parse_filter_criteria()
        scores = service.get_score_distribution("anxiety", criteria)
        png = render_score_distribution_chart("Anxiety", scores)
        return Response(png, mimetype="image/png")

    @app.route("/charts/age-group-means.png", methods=["GET"])
    def age_group_means_chart() -> Response:
        service = _build_service()
        criteria = _parse_filter_criteria()
        means = service.get_age_group_means(criteria)
        png = render_age_group_means_chart(means)
        return Response(png, mimetype="image/png")

    @app.route("/charts/music-effects.png", methods=["GET"])
    def music_effects_chart() -> Response:
        service = _build_service()
        criteria = _parse_filter_criteria()
        counts = service.get_music_effects_counts(criteria)
        png = render_music_effects_chart(counts)
        return Response(png, mimetype="image/png")

    @app.route("/export/streaming-csv", methods=["GET"])
    def export_streaming_csv() -> Any:
        """
        Export streaming service usage counts as CSV.
        """
        service = _build_service()
        counts = service.get_streaming_service_counts()

        lines = ["service,count"]
        for service_name, count in counts.items():
            lines.append(f"{service_name},{count}")

        csv_content = "\n".join(lines) + "\n"
        response = make_response(csv_content)
        response.headers["Content-Type"] = "text/csv; charset=utf-8"
        response.headers["Content-Disposition"] = "attachment; filename=streaming_counts.csv"
        return response

    return app
