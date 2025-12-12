"""
Flask web application for the Music & Mental Health Insights Tool.

Exposes a simple HTML interface over the InsightsService.
"""

from __future__ import annotations

import csv
import io
import json
import logging
import math
import os
from datetime import datetime
from typing import Any, Dict, List, Tuple
from urllib.parse import urlencode

from flask import Flask, Response, abort, current_app, g, make_response, redirect, render_template, request, url_for

from src.charts import (
    render_hours_vs_anxiety_png,
    render_age_group_means_chart,
    render_music_effects_chart,
    render_score_distribution_chart,
    render_top_genres_chart,
    render_streaming_counts_png,
    render_genre_vs_anxiety_chart,
    render_effects_vs_anxiety_chart,
    render_hours_vs_scores_chart,
)
from src.database import DatabaseManager
from src.etl_clean import clean_raw_responses_into_database
from src.etl_stage import ingest_csv_into_raw_database
from src.filters import AGE_GROUP_RULES, FilterCriteria
from src.logging_utils import configure_logger
from src.services import InsightsService
from src.models import SurveyResponse

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
RAW_EXPORT_COLUMNS = ["id", "raw_json", "ingestion_error"]
REJECTED_EXPORT_COLUMNS = ["reason", "raw_row_id", "raw_payload", "created_at"]
EXPORT_DATASETS = {
    "clean": {
        "label": "Clean respondents",
        "columns": list(EXPORTABLE_COLUMNS.keys()),
    },
    "raw": {
        "label": "Raw staging rows",
        "columns": RAW_EXPORT_COLUMNS,
    },
    "rejected": {
        "label": "Rejected rows",
        "columns": REJECTED_EXPORT_COLUMNS,
    },
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

    def _data_quality_snapshot() -> Dict[str, Any]:
        """Collect raw/clean/rejected counts and reason leaderboard."""
        manager = get_db_manager()
        return {
            "raw": manager.get_raw_row_count(),
            "clean": manager.get_clean_row_count(),
            "rejected": manager.get_rejected_row_count(),
            "top_reasons": manager.get_top_rejection_reasons(limit=5),
        }

    def _get_min_n(default: int = 5) -> int:
        """Validate min_n query parameter."""
        allowed = [1, 3, 5, 10]
        value = request.args.get("min_n")
        if value is None:
            return default
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            return default
        return parsed if parsed in allowed else default

    def _age_group_label(age: int | None) -> str:
        """Map a numeric age to a configured age group label."""
        if age is None:
            return "Unknown"
        for label, (min_age, max_age) in AGE_GROUP_RULES.items():
            if min_age is not None and age < min_age:
                continue
            if max_age is not None and age > max_age:
                continue
            return label
        return "Unknown"

    def _parse_bool_field(value: str | None) -> bool:
        """Parse yes/no form fields into booleans."""
        if value is None:
            return False
        return value.strip().lower() in {"yes", "true", "1", "on"}

    def _parse_respondent_form(
        form: Dict[str, str],
        *,
        require_response: bool,
    ) -> tuple[SurveyResponse | None, Dict[str, object] | None, Dict[str, str], List[str]]:
        """Parse respondent form data, returning typed values and validation errors."""
        errors: List[str] = []
        values = {
            "age": (form.get("age") or "").strip(),
            "streaming_service": (form.get("streaming_service") or "").strip(),
            "hours_per_day": (form.get("hours_per_day") or "").strip(),
            "fav_genre": (form.get("fav_genre") or "").strip(),
            "music_effects": (form.get("music_effects") or "").strip(),
            "while_working": (form.get("while_working") or "").strip(),
            "instrumentalist": (form.get("instrumentalist") or "").strip(),
            "composer": (form.get("composer") or "").strip(),
            "exploratory": (form.get("exploratory") or "").strip(),
            "foreign_languages": (form.get("foreign_languages") or "").strip(),
            "anxiety": (form.get("anxiety_score") or "").strip(),
            "depression": (form.get("depression_score") or "").strip(),
            "insomnia": (form.get("insomnia_score") or "").strip(),
            "ocd": (form.get("ocd_score") or "").strip(),
        }

        try:
            age = int(values["age"])
        except ValueError:
            errors.append("Age must be an integer.")
            age = None

        streaming_service = values["streaming_service"]
        if not streaming_service:
            errors.append("Streaming service is required.")

        try:
            hours_per_day = float(values["hours_per_day"])
        except ValueError:
            errors.append("Hours per day must be a number.")
            hours_per_day = 0.0

        bool_fields = {
            "while_working": _parse_bool_field(values["while_working"]),
            "instrumentalist": _parse_bool_field(values["instrumentalist"]),
            "composer": _parse_bool_field(values["composer"]),
            "exploratory": _parse_bool_field(values["exploratory"]),
            "foreign_languages": _parse_bool_field(values["foreign_languages"]),
        }

        scores: Dict[str, int] = {}
        for metric in ("anxiety", "depression", "insomnia", "ocd"):
            try:
                scores[metric] = int(values[metric])
            except ValueError:
                errors.append(f"{metric.capitalize()} score must be an integer.")

        if errors or age is None:
            return None, None, values, errors

        typed = {
            "age": age,
            "streaming_service": streaming_service,
            "hours_per_day": hours_per_day,
            "fav_genre": values["fav_genre"],
            "music_effects": values["music_effects"],
            **bool_fields,
            "anxiety": scores["anxiety"],
            "depression": scores["depression"],
            "insomnia": scores["insomnia"],
            "ocd": scores["ocd"],
        }

        response: SurveyResponse | None = None
        if require_response:
            response = SurveyResponse(
                timestamp=datetime.utcnow().isoformat(),
                age=age,
                primary_streaming_service=streaming_service,
                hours_per_day=hours_per_day,
                while_working=bool_fields["while_working"],
                instrumentalist=bool_fields["instrumentalist"],
                composer=bool_fields["composer"],
                fav_genre=values["fav_genre"],
                exploratory=bool_fields["exploratory"],
                foreign_languages=bool_fields["foreign_languages"],
                bpm=None,
                anxiety_score=scores["anxiety"],
                depression_score=scores["depression"],
                insomnia_score=scores["insomnia"],
                ocd_score=scores["ocd"],
                music_effects=values["music_effects"],
                genre_frequencies={},
            )

        return response, typed, values, []

    def _parse_limit_value(value: str | None) -> int | None:
        """Parse the export row limit respecting bounds."""
        if value is None:
            return 1000
        stripped = value.strip()
        if not stripped:
            return None
        try:
            parsed = int(stripped)
        except ValueError:
            raise ValueError("limit must be an integer between 1 and 5000")
        if parsed <= 0 or parsed > 5000:
            raise ValueError("limit must be between 1 and 5000")
        return parsed

    # --- Route definitions will be added next ---
    @app.route("/", methods=["GET"])
    def home() -> str:
        service = _get_service()
        criteria = _parse_filter_criteria()
        min_n = _get_min_n()
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

        data_quality = _data_quality_snapshot()

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
            min_n_options=[1, 3, 5, 10],
            selected_min_n=min_n,
        )

    @app.route("/data-quality", methods=["GET"])
    def data_quality() -> str:
        """Summarise staging vs cleaning counts for transparency."""
        data_quality = _data_quality_snapshot()
        cleaning_rules = [
            "Rows missing Age, Hours per day, or any mental health score are rejected.",
            "Boolean fields (While working, Instrumentalist, etc.) must be strict yes/no.",
            "Score columns must be numeric integers between 0 and 10.",
            "CSV rows are preserved verbatim in Raw staging for auditing.",
        ]
        return render_template(
            "data_quality.html",
            data_quality=data_quality,
            cleaning_rules=cleaning_rules,
        )

    @app.route("/respondents", methods=["GET"])
    def respondents_list() -> str:
        """Browse respondents with pagination and filters."""
        service = _get_service()
        criteria = _parse_filter_criteria()
        manager = get_db_manager()
        page = request.args.get("page", "1")
        try:
            page_number = max(int(page), 1)
        except ValueError:
            page_number = 1
        page_size_value = request.args.get("page_size", "10")
        if page_size_value not in {"10", "25", "50"}:
            page_size_value = "10"
        page_size = int(page_size_value)
        rows, total = manager.get_respondents_page(criteria, page_number, page_size)
        respondents: List[Dict[str, object]] = []
        for row in rows:
            respondents.append(
                {
                    "id": row["id"],
                    "age": row["age"],
                    "age_group": _age_group_label(row["age"]),
                    "streaming_service": row["primary_streaming_service"],
                    "fav_genre": row["fav_genre"] or "Unknown",
                    "music_effects": row["music_effects"],
                    "hours_bucket": _hours_bucket_value(row["hours_per_day"]),
                    "anxiety": row["anxiety"],
                    "depression": row["depression"],
                    "insomnia": row["insomnia"],
                    "ocd": row["ocd"],
                }
            )
        total_pages = max(1, math.ceil(total / page_size)) if page_size else 1
        base_params = request.args.to_dict(flat=True)
        base_params.pop("page", None)
        base_params.pop("page_size", None)
        pagination_query = urlencode(base_params)
        filter_options = service.get_filter_options()
        selected_filters = request.args.to_dict(flat=True)
        return render_template(
            "respondents.html",
            respondents=respondents,
            page=page_number,
            page_size=page_size,
            total_count=total,
            total_pages=total_pages,
            pagination_query=pagination_query,
            filter_options=filter_options,
            selected_filters=selected_filters,
            boolean_filters=BOOLEAN_FILTERS,
            hours_buckets=["<=1", "1-3", ">3"],
            page_size_value=page_size_value,
        )

    @app.route("/respondents/new", methods=["GET", "POST"])
    def respondent_new() -> Response | str:
        """Create a new respondent record."""
        errors: List[str] = []
        form_values = {
            "age": "",
            "streaming_service": "",
            "hours_per_day": "",
            "fav_genre": "",
            "music_effects": "",
            "while_working": "",
            "instrumentalist": "",
            "composer": "",
            "exploratory": "",
            "foreign_languages": "",
            "anxiety": "",
            "depression": "",
            "insomnia": "",
            "ocd": "",
        }
        if request.method == "POST":
            response_obj, typed, values, errors = _parse_respondent_form(request.form, require_response=True)
            form_values = values
            if not errors and response_obj and typed:
                manager = get_db_manager()
                respondent_id = manager.insert_survey_response(response_obj)
                return redirect(url_for("respondent_detail", respondent_id=respondent_id))
        return render_template(
            "respondent_form.html",
            form_mode="create",
            submit_label="Create respondent",
            form_values=form_values,
            errors=errors,
            form_action=url_for("respondent_new"),
        )

    @app.route("/respondents/<int:respondent_id>", methods=["GET"])
    def respondent_detail(respondent_id: int) -> str:
        """Display a single respondent record."""
        manager = get_db_manager()
        row = manager.get_respondent_with_scores(respondent_id)
        if row is None:
            abort(404)
        respondent = {
            "id": row["id"],
            "age": row["age"],
            "age_group": _age_group_label(row["age"]),
            "streaming_service": row["primary_streaming_service"],
            "fav_genre": row["fav_genre"] or "Unknown",
            "music_effects": row["music_effects"],
            "hours_per_day": row["hours_per_day"],
            "hours_bucket": _hours_bucket_value(row["hours_per_day"]),
            "while_working": bool(row["while_working"]),
            "instrumentalist": bool(row["instrumentalist"]),
            "composer": bool(row["composer"]),
            "exploratory": bool(row["exploratory"]),
            "foreign_languages": bool(row["foreign_languages"]),
            "anxiety": row["anxiety"],
            "depression": row["depression"],
            "insomnia": row["insomnia"],
            "ocd": row["ocd"],
        }
        return render_template("respondent_detail.html", respondent=respondent)

    @app.route("/respondents/<int:respondent_id>/edit", methods=["GET", "POST"])
    def respondent_edit(respondent_id: int) -> Response | str:
        """Edit an existing respondent."""
        manager = get_db_manager()
        row = manager.get_respondent_with_scores(respondent_id)
        if row is None:
            abort(404)
        errors: List[str] = []
        if request.method == "POST":
            _, typed, values, errors = _parse_respondent_form(request.form, require_response=False)
            form_values = values
            if not errors and typed:
                respondent_data = {
                    "age": typed["age"],
                    "streaming_service": typed["streaming_service"],
                    "hours_per_day": typed["hours_per_day"],
                    "fav_genre": typed["fav_genre"],
                    "music_effects": typed["music_effects"],
                    "while_working": typed["while_working"],
                    "instrumentalist": typed["instrumentalist"],
                    "composer": typed["composer"],
                    "exploratory": typed["exploratory"],
                    "foreign_languages": typed["foreign_languages"],
                }
                score_data = {
                    "anxiety": typed["anxiety"],
                    "depression": typed["depression"],
                    "insomnia": typed["insomnia"],
                    "ocd": typed["ocd"],
                }
                manager.update_respondent_with_scores(respondent_id, respondent_data, score_data)
                return redirect(url_for("respondent_detail", respondent_id=respondent_id))
        else:
            form_values = {
                "age": str(row["age"]),
                "streaming_service": row["primary_streaming_service"],
                "hours_per_day": str(row["hours_per_day"]),
                "fav_genre": row["fav_genre"] or "",
                "music_effects": row["music_effects"] or "",
                "while_working": "yes" if row["while_working"] else "no",
                "instrumentalist": "yes" if row["instrumentalist"] else "no",
                "composer": "yes" if row["composer"] else "no",
                "exploratory": "yes" if row["exploratory"] else "no",
                "foreign_languages": "yes" if row["foreign_languages"] else "no",
                "anxiety": str(row["anxiety"]),
                "depression": str(row["depression"]),
                "insomnia": str(row["insomnia"]),
                "ocd": str(row["ocd"]),
            }
        return render_template(
            "respondent_form.html",
            form_mode="edit",
            submit_label="Save changes",
            form_values=form_values,
            errors=errors,
            form_action=url_for("respondent_edit", respondent_id=respondent_id),
        )

    @app.route("/respondents/<int:respondent_id>/delete", methods=["POST"])
    def respondent_delete(respondent_id: int) -> Response:
        """Delete a respondent and related health stats."""
        manager = get_db_manager()
        manager.delete_respondent_and_health_stats(respondent_id)
        return redirect(url_for("respondents_list"))

    @app.route("/health-impact", methods=["GET"])
    def health_impact() -> str:
        """Render focused health impact analytics and charts."""
        service = _get_service()
        criteria = _parse_filter_criteria()
        min_n = _get_min_n()
        filter_options = service.get_filter_options()
        genre_stats = service.get_mean_scores_by_genre(filters=criteria, min_n=min_n)
        effect_stats = service.get_mean_scores_by_music_effects(filters=criteria, min_n=min_n)
        hours_stats = service.get_mean_scores_by_hours_bucket(filters=criteria, min_n=min_n)
        correlations = service.get_correlations_hours_vs_scores(filters=criteria)
        selected_filters = request.args.to_dict(flat=True)
        query_string = request.query_string.decode("utf-8")

        def _chart_url(endpoint: str) -> str:
            base = url_for(endpoint)
            return f"{base}?{query_string}" if query_string else base

        charts = {
            "genres": _chart_url("genre_vs_anxiety_chart"),
            "effects": _chart_url("effects_vs_anxiety_chart"),
            "hours": _chart_url("hours_vs_scores_chart"),
        }

        return render_template(
            "health_impact.html",
            genre_stats=genre_stats,
            effect_stats=effect_stats,
            hours_stats=hours_stats,
            correlations=correlations,
            charts=charts,
            filter_options=filter_options,
            selected_filters=selected_filters,
            boolean_filters=BOOLEAN_FILTERS,
            hours_buckets=["<=1", "1-3", ">3"],
            min_n_options=[1, 3, 5, 10],
            selected_min_n=min_n,
        )

    @app.route("/genre", methods=["GET", "POST"])
    def genre_insights() -> str:
        """Provide a deeper dive into a specific favourite genre."""
        service = _get_service()
        min_n = _get_min_n()
        filter_options = service.get_filter_options()
        genre_options = sorted(filter_options["genres"])
        if "Unknown" not in genre_options:
            genre_options.append("Unknown")
        score_metrics = [
            ("anxiety", "Anxiety"),
            ("depression", "Depression"),
            ("insomnia", "Insomnia"),
            ("ocd", "OCD"),
        ]
        metric_lookup = {key: label for key, label in score_metrics}

        selected_genre = ""
        selected_metric = (request.values.get("metric") or "anxiety").lower()
        if selected_metric not in metric_lookup:
            selected_metric = "anxiety"

        if request.method == "POST":
            selected_genre = request.form.get("genre", "").strip()
        else:
            selected_genre = request.args.get("genre", "").strip()

        genre_stats_list = service.get_mean_scores_by_genre(filters=None, min_n=1)
        genre_stats_map = {row["genre"]: row for row in genre_stats_list}
        stats_entry = None
        warning_message = None
        chart_url: str | None = None
        deltas: Dict[str, float] = {}
        overall_means = service.get_overview().get("mean_scores", {})

        if selected_genre:
            key = selected_genre
            stats_entry = genre_stats_map.get(key)
            if stats_entry:
                chart_url = url_for(
                    "genre_distribution_chart",
                    genre=key,
                    metric=selected_metric,
                )
                deltas = {
                    "anxiety": stats_entry["anxiety_mean"] - overall_means.get("anxiety", 0.0),
                    "depression": stats_entry["depression_mean"] - overall_means.get("depression", 0.0),
                    "insomnia": stats_entry["insomnia_mean"] - overall_means.get("insomnia", 0.0),
                    "ocd": stats_entry["ocd_mean"] - overall_means.get("ocd", 0.0),
                }
                if stats_entry["n"] < min_n:
                    warning_message = (
                        f"Only {stats_entry['n']} respondents meet the filters; "
                        f"min_n={min_n}. Rankings and comparisons may be noisy."
                    )

        return render_template(
            "genre.html",
            genre_options=genre_options,
            selected_genre=selected_genre,
            stats_entry=stats_entry,
            warning_message=warning_message,
            chart_url=chart_url,
            metric_labels=score_metrics,
            metric_lookup=metric_lookup,
            selected_metric=selected_metric,
            deltas=deltas,
            overall_means=overall_means,
            min_n_options=[1, 3, 5, 10],
            selected_min_n=min_n,
        )


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

    @app.route("/charts/genre-distribution.png", methods=["GET"])
    def genre_distribution_chart() -> Response:
        service = _get_service()
        metric = (request.args.get("metric") or "anxiety").lower()
        metric_labels = {
            "anxiety": "Anxiety",
            "depression": "Depression",
            "insomnia": "Insomnia",
            "ocd": "OCD",
        }
        if metric not in metric_labels:
            metric = "anxiety"
        genre = request.args.get("genre", "").strip() or "Unknown"
        responses = service.get_responses_by_genre(genre)
        distribution: Dict[int, int] = {}
        score_attr = f"{metric}_score"
        for response in responses:
            score = getattr(response, score_attr)
            distribution[score] = distribution.get(score, 0) + 1
        if not distribution:
            distribution = {0: 0}
        png = render_score_distribution_chart(metric_labels[metric], distribution)
        return Response(png, mimetype="image/png")

    @app.route("/charts/genre-vs-anxiety.png", methods=["GET"])
    def genre_vs_anxiety_chart() -> Response:
        service = _get_service()
        criteria = _parse_filter_criteria()
        min_n = _get_min_n()
        stats = service.get_mean_scores_by_genre(filters=criteria, min_n=min_n)
        png = render_genre_vs_anxiety_chart(stats)
        return Response(png, mimetype="image/png")

    @app.route("/charts/effects-vs-anxiety.png", methods=["GET"])
    def effects_vs_anxiety_chart() -> Response:
        service = _get_service()
        criteria = _parse_filter_criteria()
        min_n = _get_min_n()
        stats = service.get_mean_scores_by_music_effects(filters=criteria, min_n=min_n)
        png = render_effects_vs_anxiety_chart(stats)
        return Response(png, mimetype="image/png")

    @app.route("/charts/hours-vs-scores.png", methods=["GET"])
    def hours_vs_scores_chart() -> Response:
        service = _get_service()
        criteria = _parse_filter_criteria()
        min_n = _get_min_n()
        stats = service.get_mean_scores_by_hours_bucket(filters=criteria, min_n=min_n)
        png = render_hours_vs_scores_chart(stats)
        return Response(png, mimetype="image/png")

    @app.route("/export/rejected.csv", methods=["GET"])
    def export_rejected_rows() -> Response:
        """Download rejected rows with reasons and raw payload."""
        manager = get_db_manager()
        cursor = manager.connection.cursor()
        cursor.execute(
            """
            SELECT reason, raw_row_id, raw_payload
            FROM RejectedRows
            ORDER BY id ASC
            """
        )
        rows = cursor.fetchall()
        lines = ["reason,raw_row_id,raw_payload"]
        for row in rows:
            reason = (row["reason"] or "").replace('"', '""')
            raw_row_id = row["raw_row_id"] if row["raw_row_id"] is not None else ""
            payload = (row["raw_payload"] or "").replace('"', '""')
            lines.append(f'"{reason}",{raw_row_id},"{payload}"')
        csv_data = "\n".join(lines) + "\n"
        response = make_response(csv_data)
        response.headers["Content-Type"] = "text/csv; charset=utf-8"
        response.headers["Content-Disposition"] = "attachment; filename=rejected_rows.csv"
        return response

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
        dataset = selected_filters.get("dataset", "clean")
        if dataset not in EXPORT_DATASETS:
            dataset = "clean"
        available_columns = EXPORT_DATASETS[dataset]["columns"]
        selected_columns = request.args.getlist("columns") or available_columns
        limit_value = selected_filters.get("limit", "1000")
        return render_template(
            "export.html",
            filter_options=filter_options,
            selected_filters=selected_filters,
            available_columns=available_columns,
            selected_columns=selected_columns,
            dataset=dataset,
            dataset_options=EXPORT_DATASETS,
            limit_value=limit_value,
            boolean_filters=BOOLEAN_FILTERS,
            hours_buckets=["<=1", "1-3", ">3"],
        )

    @app.route("/export/download.csv", methods=["GET"])
    def export_download() -> Response:
        """Stream CSV exports for clean, raw, or rejected datasets."""
        dataset = request.args.get("dataset", "clean")
        if dataset not in EXPORT_DATASETS:
            return Response("Unknown dataset", status=400)
        selected_columns = request.args.getlist("columns") or EXPORT_DATASETS[dataset]["columns"]
        invalid = [col for col in selected_columns if col not in EXPORT_DATASETS[dataset]["columns"]]
        if invalid:
            return Response(f"Invalid columns: {', '.join(invalid)}", status=400)
        try:
            limit = _parse_limit_value(request.args.get("limit"))
        except ValueError as exc:
            return Response(str(exc), status=400)

        manager = get_db_manager()

        def _stream(row_iter: Any) -> Response:
            def generate() -> Any:
                output = io.StringIO()
                writer = csv.writer(output)
                writer.writerow(selected_columns)
                yield output.getvalue()
                output.seek(0)
                output.truncate(0)
                for row in row_iter:
                    writer.writerow(row)
                    yield output.getvalue()
                    output.seek(0)
                    output.truncate(0)

            response = Response(generate(), mimetype="text/csv")
            response.headers["Content-Type"] = "text/csv; charset=utf-8"
            response.headers["Content-Disposition"] = f"attachment; filename={dataset}_export.csv"
            return response

        if dataset == "clean":
            criteria = _parse_filter_criteria()
            responses = manager.get_clean_responses_filtered(criteria, limit=limit)
            def row_iter() -> Any:
                for response in responses:
                    record: List[Any] = []
                    for column in selected_columns:
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
                        record.append(value)
                    yield record

            return _stream(row_iter())

        if dataset == "raw":
            raw_rows = manager.get_raw_rows(limit)
            def row_iter_raw() -> Any:
                for raw in raw_rows:
                    yield [raw[column] if raw[column] is not None else "" for column in selected_columns]

            return _stream(row_iter_raw())

        rejected_rows = manager.get_rejected_rows(limit)
        def row_iter_rejected() -> Any:
            for rejected in rejected_rows:
                yield [rejected[column] if rejected[column] is not None else "" for column in selected_columns]

        return _stream(row_iter_rejected())

    @app.teardown_appcontext
    def close_db_manager(_: Exception | None) -> None:
        """Close any request-scoped database managers."""
        manager: DatabaseManager | None = g.pop("db_manager", None)
        if manager is not None:
            manager.close()
        g.pop("insights_service", None)

    return app
