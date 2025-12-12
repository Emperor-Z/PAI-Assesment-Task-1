"""
Flask web application for the Music & Mental Health Insights Tool.

Exposes a simple HTML interface over the InsightsService.
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os
from typing import Any, Dict, List, Tuple

from flask import Flask, Response, current_app, g, make_response, render_template, request, url_for

from src.charts import (
    render_hours_vs_anxiety_png,
    render_age_group_means_chart,
    render_music_effects_chart,
    render_score_distribution_chart,
    render_top_genres_chart,
    render_streaming_counts_png,
)
from src.database import DatabaseManager
from src.etl_clean import clean_raw_responses_into_database
from src.etl_stage import ingest_csv_into_raw_database
from src.filters import FilterCriteria
from src.logging_utils import configure_logger
from src.services import InsightsService

BOOLEAN_FILTERS: List[Tuple[str, str]] = [
    ("while_working", "While working"),
    ("exploratory", "Exploratory listener"),
    ("foreign_languages", "Foreign languages"),
    ("instrumentalist", "Instrumentalist"),
    ("composer", "Composer"),
]
EXPORTABLE_COLUMNS: Dict[str, str] = {
    "timestamp": "timestamp",
    "age": "age",
    "primary_streaming_service": "primary_streaming_service",
    "hours_per_day": "hours_per_day",
    "while_working": "while_working",
    "instrumentalist": "instrumentalist",
    "composer": "composer",
    "fav_genre": "fav_genre",
    "exploratory": "exploratory",
    "foreign_languages": "foreign_languages",
    "bpm": "bpm",
    "anxiety_score": "anxiety_score",
    "depression_score": "depression_score",
    "insomnia_score": "insomnia_score",
    "ocd_score": "ocd_score",
    "music_effects": "music_effects",
    "hours_per_day_bucket": "hours_per_day",  # calculated bucket
}


def _hours_bucket_value(hours: float) -> str:
    """Helper to convert numeric hours into the dashboard buckets."""
    if hours <= 1:
        return "<=1"
    if hours <= 3:
        return "1-3"
    return ">3"


def get_db_manager() -> DatabaseManager:
    """
    Return a request-scoped DatabaseManager stored on flask.g.

    The manager is initialised lazily per request and automatically
    bootstraps the SQLite database (staging + cleaning) if empty.
    """
    manager: DatabaseManager | None = g.get("db_manager")
    if manager is not None:
        return manager

    db_path = current_app.config["DB_PATH"]
    manager = DatabaseManager(db_path)
    manager.connect()
    manager.create_tables()

    if manager.get_respondent_count() == 0:
        csv_path = current_app.config["CSV_PATH"]
        ingest_csv_into_raw_database(csv_path, manager)
        clean_raw_responses_into_database(manager)

    g.db_manager = manager
    return manager


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

    def _get_service() -> InsightsService:
        """Return a request-scoped InsightsService bound to the DB manager."""
        service: InsightsService | None = g.get("insights_service")
        if service is None:
            service = InsightsService(get_db_manager())
            g.insights_service = service
        return service

    def _parse_filter_criteria() -> FilterCriteria:
        """Build filter criteria from query parameters."""
        return FilterCriteria.from_request_args(request.args.to_dict(flat=True))

    # --- Route definitions will be added next ---
    @app.route("/", methods=["GET"])
    def home() -> str:
        service = _get_service()
        manager = get_db_manager()
        criteria = _parse_filter_criteria()
        filter_options = service.get_filter_options()
        overview = service.get_overview(criteria)
        streaming_counts = overview.get("streaming_counts", {})
        top_services = sorted(
            streaming_counts.items(),
            key=lambda item: (-item[1], item[0]),
        )[:5]
        top_genres = service.get_top_genres(criteria, top_n=5)
        mean_scores = overview.get("mean_scores", {})
        selected_filters = request.args.to_dict(flat=True)
        query_string = request.query_string.decode("utf-8")

        def _chart_url(endpoint: str) -> str:
            base = url_for(endpoint)
            return f"{base}?{query_string}" if query_string else base

        charts = {
            "streaming": _chart_url("streaming_chart"),
            "hours": _chart_url("hours_vs_anxiety_chart"),
            "anxiety": _chart_url("anxiety_distribution_chart"),
            "age_groups": _chart_url("age_group_means_chart"),
            "music_effects": _chart_url("music_effects_chart"),
            "top_genres": _chart_url("top_genres_chart"),
        }

        data_quality = {
            "raw": manager.get_raw_row_count(),
            "clean": manager.get_clean_row_count(),
            "rejected": manager.get_rejected_row_count(),
            "top_reasons": manager.get_top_rejection_reasons(limit=5),
        }

        return render_template(
            "home.html",
            overview=overview,
            filter_options=filter_options,
            selected_filters=selected_filters,
            top_services=top_services,
            top_genres=top_genres,
            mean_scores=mean_scores,
            charts=charts,
            data_quality=data_quality,
            boolean_filters=BOOLEAN_FILTERS,
            hours_buckets=["<=1", "1-3", ">3"],
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
            service = _get_service()
            service_stats = service.get_average_anxiety_and_depression_by_genre(genre)
            stats = {
                "genre": service_stats.get("genre", genre),
                "avg_anxiety": service_stats.get("avg_anxiety", 0.0),
                "avg_depression": service_stats.get("avg_depression", 0.0),
            }

        return render_template("genre.html", stats=stats)


    @app.route("/streaming", methods=["GET"])
    def streaming_counts() -> str:
        service = _get_service()
        counts = service.get_streaming_service_counts()
        return render_template("streaming.html", counts=counts)

    @app.route("/hours-vs-anxiety", methods=["GET"])
    def hours_vs_anxiety() -> str:
        service = _get_service()
        buckets = service.get_hours_vs_anxiety()
        return render_template("hours_vs_anxiety.html", buckets=buckets)

    @app.route("/charts/streaming.png", methods=["GET"])
    def streaming_chart() -> Response:
        service = _get_service()
        counts = service.get_streaming_service_counts()
        png = render_streaming_counts_png(counts)
        return Response(png, mimetype="image/png")

    @app.route("/charts/hours-vs-anxiety.png", methods=["GET"])
    def hours_vs_anxiety_chart() -> Response:
        service = _get_service()
        buckets = service.get_hours_vs_anxiety()
        png = render_hours_vs_anxiety_png(buckets)
        return Response(png, mimetype="image/png")

    @app.route("/charts/anxiety-distribution.png", methods=["GET"])
    def anxiety_distribution_chart() -> Response:
        service = _get_service()
        criteria = _parse_filter_criteria()
        scores = service.get_score_distribution("anxiety", criteria)
        png = render_score_distribution_chart("Anxiety", scores)
        return Response(png, mimetype="image/png")

    @app.route("/charts/age-group-means.png", methods=["GET"])
    def age_group_means_chart() -> Response:
        service = _get_service()
        criteria = _parse_filter_criteria()
        means = service.get_age_group_means(criteria)
        png = render_age_group_means_chart(means)
        return Response(png, mimetype="image/png")

    @app.route("/charts/music-effects.png", methods=["GET"])
    def music_effects_chart() -> Response:
        service = _get_service()
        criteria = _parse_filter_criteria()
        counts = service.get_music_effects_counts(criteria)
        png = render_music_effects_chart(counts)
        return Response(png, mimetype="image/png")

    @app.route("/charts/top-genres.png", methods=["GET"])
    def top_genres_chart() -> Response:
        service = _get_service()
        criteria = _parse_filter_criteria()
        top_genres = service.get_top_genres(criteria)
        png = render_top_genres_chart(top_genres)
        return Response(png, mimetype="image/png")

    @app.route("/export/streaming-csv", methods=["GET"])
    def export_streaming_csv() -> Any:
        """
        Export streaming service usage counts as CSV.
        """
        service = _get_service()
        counts = service.get_streaming_service_counts()

        lines = ["service,count"]
        for service_name, count in counts.items():
            lines.append(f"{service_name},{count}")

        csv_content = "\n".join(lines) + "\n"
        response = make_response(csv_content)
        response.headers["Content-Type"] = "text/csv; charset=utf-8"
        response.headers["Content-Disposition"] = "attachment; filename=streaming_counts.csv"
        return response

    @app.route("/export", methods=["GET"])
    def export_page() -> str:
        """Render a configuration page for data exports."""
        service = _get_service()
        filter_options = service.get_filter_options()
        selected_filters = request.args.to_dict(flat=True)
        return render_template(
            "export.html",
            filter_options=filter_options,
            selected_filters=selected_filters,
            export_columns=list(EXPORTABLE_COLUMNS.keys()),
            boolean_filters=BOOLEAN_FILTERS,
            hours_buckets=["<=1", "1-3", ">3"],
        )

    @app.route("/export/data.csv", methods=["GET"])
    def export_filtered_data() -> Response:
        """Export filtered curated responses with configurable columns."""
        service = _get_service()
        criteria = _parse_filter_criteria()
        columns_param = request.args.get("columns", "")
        column_args = request.args.getlist("columns")
        if column_args:
            requested_columns = []
            for entry in column_args:
                requested_columns.extend(
                    [col.strip() for col in entry.split(",") if col.strip()]
                )
        elif columns_param:
            requested_columns = [col.strip() for col in columns_param.split(",") if col.strip()]
        else:
            requested_columns = list(EXPORTABLE_COLUMNS.keys())

        if not requested_columns:
            return Response("At least one column must be selected", status=400)

        invalid_columns = [col for col in requested_columns if col not in EXPORTABLE_COLUMNS]
        if invalid_columns:
            return Response(f"Invalid columns: {', '.join(invalid_columns)}", status=400)

        limit_value = request.args.get("limit")
        limit: int | None = None
        if limit_value:
            try:
                limit = int(limit_value)
            except ValueError:
                return Response("limit must be an integer", status=400)
            if limit <= 0 or limit > 5000:
                return Response("limit must be between 1 and 5000", status=400)

        responses = service.db_manager.get_clean_responses_filtered(criteria, limit=limit)

        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=requested_columns)
        writer.writeheader()
        for response in responses:
            row: Dict[str, Any] = {}
            for column in requested_columns:
                attr = EXPORTABLE_COLUMNS[column]
                value = getattr(response, attr)
                if column == "hours_per_day_bucket":
                    value = _hours_bucket_value(getattr(response, attr))
                elif isinstance(value, bool):
                    value = "yes" if value else "no"
                elif isinstance(value, dict):
                    value = json.dumps(value)
                elif value is None:
                    value = ""
                row[column] = value
            writer.writerow(row)

        logger = logging.getLogger("music_health_app")
        logger.info(
            "action=EXPORT_DATA columns=%s limit=%s",
            ",".join(requested_columns),
            limit or "all",
        )

        csv_data = buffer.getvalue()
        response = make_response(csv_data)
        response.headers["Content-Type"] = "text/csv; charset=utf-8"
        response.headers["Content-Disposition"] = "attachment; filename=filtered_responses.csv"
        return response

    @app.teardown_appcontext
    def close_db_manager(_: Exception | None) -> None:
        """Close any request-scoped database managers."""
        manager: DatabaseManager | None = g.pop("db_manager", None)
        if manager is not None:
            manager.close()
        g.pop("insights_service", None)

    return app
