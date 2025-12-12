"""
Service layer for the Music & Mental Health Insights Tool.

InsightsService wraps the AnalysisEngine to provide higher-level
operations that will later be used by controllers or Flask routes.
All public methods are decorated with log_action so that each call is
captured in the application log.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List

from src.database import DatabaseManager
from src.filters import AGE_GROUP_RULES, FilterCriteria
from src.logging_utils import log_action
from src.models import AnalysisEngine, SurveyResponse


@dataclass
class InsightsService:
    """Provide analytics-friendly operations backed by curated data."""

    data_source: DatabaseManager | List[SurveyResponse]
    db_manager: DatabaseManager | None = field(default=None, init=False)
    _responses_cache: List[SurveyResponse] | None = field(default=None, init=False)
    _engine: AnalysisEngine | None = field(default=None, init=False)

    def __post_init__(self) -> None:
        if isinstance(self.data_source, DatabaseManager):
            self.db_manager = self.data_source
        else:
            self.db_manager = None
            self._responses_cache = list(self.data_source)
            self._engine = AnalysisEngine(self._responses_cache)

    def _get_responses(self) -> List[SurveyResponse]:
        """Lazy-load SurveyResponse objects from the database."""
        if self._responses_cache is None:
            manager = self._require_db_manager()
            self._responses_cache = manager.get_all_clean_responses()
        return self._responses_cache

    def _get_engine(self) -> AnalysisEngine:
        """Return an AnalysisEngine backed by the cached responses."""
        if self._engine is None:
            self._engine = AnalysisEngine(self._get_responses())
        return self._engine

    def _get_engine_for(self, responses: List[SurveyResponse]) -> AnalysisEngine:
        """Return an AnalysisEngine for the provided dataset."""
        if self._responses_cache is not None and responses is self._responses_cache:
            return self._get_engine()
        return AnalysisEngine(responses)

    def _get_responses_for(self, criteria: FilterCriteria | None) -> List[SurveyResponse]:
        """Resolve the dataset matching the provided criteria."""
        if criteria and self._criteria_has_filters(criteria):
            manager = self._require_db_manager()
            return manager.get_clean_responses_filtered(criteria)
        return self._get_responses()

    @staticmethod
    def _criteria_has_filters(criteria: FilterCriteria) -> bool:
        """Determine whether any filter fields are active."""
        return any(value is not None for value in criteria.__dict__.values())

    def _require_db_manager(self) -> DatabaseManager:
        """Return the DB manager or raise if unavailable."""
        if self.db_manager is None:
            raise RuntimeError("Database operations require a DatabaseManager.")
        return self.db_manager

    @log_action("GET_GENRE_MENTAL_HEALTH_STATS")
    def get_average_anxiety_and_depression_by_genre(self, genre: str) -> Dict[str, float]:
        engine = self._get_engine()
        avg_anxiety = engine.get_average_anxiety_by_genre(genre) or 0.0
        avg_depression = engine.get_average_depression_by_genre(genre) or 0.0
        return {
            "genre": genre,
            "avg_anxiety": avg_anxiety,
            "avg_depression": avg_depression,
        }

    @log_action("GET_STREAMING_COUNTS")
    def get_streaming_service_counts(self) -> Dict[str, int]:
        engine = self._get_engine()
        return engine.get_count_by_streaming_service()

    @log_action("GET_HOURS_VS_ANXIETY")
    def get_hours_vs_anxiety(self) -> Dict[str, float]:
        engine = self._get_engine()
        return engine.get_hours_per_day_vs_average_anxiety()

    @log_action("GET_OVERVIEW")
    def get_overview(self, criteria: FilterCriteria | None = None) -> Dict[str, Any]:
        """Return headline stats for the dashboard, optionally filtered."""
        responses = self._get_responses_for(criteria)
        total = len(responses)
        streaming_counts: Dict[str, int] = {}
        for response in responses:
            service = response.primary_streaming_service
            streaming_counts[service] = streaming_counts.get(service, 0) + 1

        top_service = max(streaming_counts, key=streaming_counts.get) if streaming_counts else "N/A"

        mean_scores = self._compute_mean_scores(responses)
        return {
            "total_count": total,
            "top_streaming_service": top_service,
            "streaming_counts": streaming_counts,
            "mean_scores": mean_scores,
        }

    @log_action("GET_AGE_GROUP_MEANS")
    def get_age_group_means(
        self,
        criteria: FilterCriteria | None = None,
    ) -> Dict[str, Dict[str, float]]:
        """Return mean mental-health scores per age bucket."""
        responses = self._get_responses_for(criteria)
        engine = self._get_engine_for(responses)
        return engine.get_age_group_means(responses)

    def _compute_mean_scores(self, responses: List[SurveyResponse]) -> Dict[str, float]:
        """Compute average anxiety/depression/insomnia/OCD scores."""
        if not responses:
            return {"anxiety": 0.0, "depression": 0.0, "insomnia": 0.0, "ocd": 0.0}

        def _mean(values: List[int]) -> float:
            return sum(values) / len(values) if values else 0.0

        return {
            "anxiety": _mean([r.anxiety_score for r in responses]),
            "depression": _mean([r.depression_score for r in responses]),
            "insomnia": _mean([r.insomnia_score for r in responses]),
            "ocd": _mean([r.ocd_score for r in responses]),
        }

    @log_action("GET_SCORE_DISTRIBUTION")
    def get_score_distribution(
        self,
        metric: str,
        criteria: FilterCriteria | None = None,
    ) -> Dict[int, int]:
        """Return score distribution for a metric, optionally filtered."""
        responses = self._get_responses_for(criteria)
        engine = self._get_engine_for(responses)
        return engine.get_score_distribution(metric, responses)

    @log_action("GET_MUSIC_EFFECTS_COUNTS")
    def get_music_effects_counts(
        self,
        criteria: FilterCriteria | None = None,
    ) -> Dict[str, int]:
        """Return counts of music effects descriptions."""
        responses = self._get_responses_for(criteria)
        engine = self._get_engine_for(responses)
        return engine.get_music_effects_counts(responses)

    @log_action("GET_TOP_GENRES")
    def get_top_genres(
        self,
        criteria: FilterCriteria | None = None,
        top_n: int = 10,
    ) -> List[tuple[str, int]]:
        """Return the most popular favourite genres."""
        if top_n <= 0:
            raise ValueError("top_n must be a positive integer")
        responses = self._get_responses_for(criteria)
        engine = self._get_engine_for(responses)
        return engine.get_top_genres(responses, top_n=top_n)

    @log_action("GET_GENRE_MEANS")
    def get_genre_means(
        self,
        metric: str,
        criteria: FilterCriteria | None = None,
        top_n: int = 10,
    ) -> List[tuple[str, float]]:
        """Return mean metric scores for the most popular genres."""
        responses = self._get_responses_for(criteria)
        engine = self._get_engine_for(responses)
        return engine.get_genre_means(metric, responses, top_n=top_n)

    @log_action("GET_FACTOR_MEANS")
    def get_factor_means(
        self,
        factor: str,
        metric: str,
        criteria: FilterCriteria | None = None,
    ) -> Dict[bool, float]:
        """Return mean metric scores split by boolean factor."""
        responses = self._get_responses_for(criteria)
        engine = self._get_engine_for(responses)
        return engine.get_factor_means(factor, metric, responses)

    @log_action("GET_FILTER_OPTIONS")
    def get_filter_options(self) -> Dict[str, List[str]]:
        """Return dropdown options for the dashboard filters."""
        manager = self._require_db_manager()
        services = manager.get_distinct_streaming_services()
        genres = manager.get_distinct_fav_genres()
        effects = manager.get_distinct_music_effects()
        return {
            "streaming_services": services,
            "genres": genres,
            "music_effects": effects,
            "age_groups": list(AGE_GROUP_RULES.keys()),
        }

    def get_responses_by_genre(self, genre: str) -> List[SurveyResponse]:
        """Return curated responses matching a favourite genre label."""
        normalised = (genre or "Unknown").strip() or "Unknown"
        responses = self._get_responses()
        return [resp for resp in responses if (resp.fav_genre or "Unknown") == normalised]

    @log_action("GET_MEAN_SCORES_BY_GENRE")
    def get_mean_scores_by_genre(
        self,
        top_n: int = 10,
        min_n: int = 5,
        filters: FilterCriteria | None = None,
    ) -> List[Dict[str, Any]]:
        """Return mean mental health scores grouped by favourite genre."""
        responses = self._get_responses_for(filters)
        grouped = self._group_mean_stats(
            responses,
            key_func=lambda r: r.fav_genre or "Unknown",
        )
        rows = [
            {"genre": genre, **stats}
            for genre, stats in grouped.items()
            if stats["n"] >= min_n
        ]
        rows.sort(key=lambda row: (-row["n"], row["genre"]))
        return rows[:top_n]

    @log_action("GET_MEAN_SCORES_BY_EFFECT")
    def get_mean_scores_by_music_effects(
        self,
        min_n: int = 5,
        filters: FilterCriteria | None = None,
    ) -> List[Dict[str, Any]]:
        """Return mean scores grouped by music effects self-report."""
        responses = self._get_responses_for(filters)
        grouped = self._group_mean_stats(
            responses,
            key_func=lambda r: r.music_effects or "Unknown",
        )
        rows = [
            {"effect": effect, **stats}
            for effect, stats in grouped.items()
            if stats["n"] >= min_n
        ]
        rows.sort(key=lambda row: (-row["n"], row["effect"]))
        return rows

    @log_action("GET_MEAN_SCORES_BY_HOURS_BUCKET")
    def get_mean_scores_by_hours_bucket(
        self,
        min_n: int = 5,
        filters: FilterCriteria | None = None,
    ) -> List[Dict[str, Any]]:
        """Return mean scores grouped by listening hours bucket."""
        responses = self._get_responses_for(filters)
        grouped = self._group_mean_stats(
            responses,
            key_func=lambda r: self._hours_bucket(r.hours_per_day),
        )
        rows = [
            {"bucket": bucket, **stats}
            for bucket, stats in grouped.items()
            if stats["n"] >= min_n
        ]
        order = {"<=1": 0, "1-3": 1, ">3": 2}
        rows.sort(key=lambda row: (order.get(row["bucket"], 3), -row["n"]))
        return rows

    @log_action("GET_HOURS_VS_SCORE_CORRELATIONS")
    def get_correlations_hours_vs_scores(
        self,
        filters: FilterCriteria | None = None,
    ) -> Dict[str, Dict[str, float | int | None]]:
        """Return Pearson correlations between hours per day and each score."""
        responses = self._get_responses_for(filters)
        hours = [resp.hours_per_day for resp in responses]
        metrics = {
            "anxiety": [resp.anxiety_score for resp in responses],
            "depression": [resp.depression_score for resp in responses],
            "insomnia": [resp.insomnia_score for resp in responses],
            "ocd": [resp.ocd_score for resp in responses],
        }
        results: Dict[str, Dict[str, float | int | None]] = {}
        for name, values in metrics.items():
            pairs = [(x, y) for x, y in zip(hours, values) if x is not None and y is not None]
            n = len(pairs)
            if n < 2:
                results[name] = {"n": n, "r": None}
                continue
            x_vals, y_vals = zip(*pairs)
            r = self._pearson_correlation(list(x_vals), list(y_vals))
            results[name] = {"n": n, "r": r}
        return results

    @staticmethod
    def _pearson_correlation(xs: List[float], ys: List[float]) -> float | None:
        """Compute Pearson correlation coefficient."""
        n = len(xs)
        if n < 2:  # pragma: no cover - guarded earlier
            return None
        mean_x = sum(xs) / n
        mean_y = sum(ys) / n
        cov = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
        denom_x = sum((x - mean_x) ** 2 for x in xs)
        denom_y = sum((y - mean_y) ** 2 for y in ys)
        if denom_x == 0 or denom_y == 0:
            return None
        return cov / (denom_x ** 0.5 * denom_y ** 0.5)

    def _group_mean_stats(
        self,
        responses: List[SurveyResponse],
        key_func,
    ) -> Dict[str, Dict[str, float]]:
        """Aggregate responses and calculate mean scores per group."""
        grouped: Dict[str, Dict[str, float]] = {}
        for response in responses:
            key = key_func(response)
            stats = grouped.setdefault(
                key,
                {
                    "n": 0,
                    "anxiety_sum": 0.0,
                    "depression_sum": 0.0,
                    "insomnia_sum": 0.0,
                    "ocd_sum": 0.0,
                },
            )
            stats["n"] += 1
            stats["anxiety_sum"] += response.anxiety_score
            stats["depression_sum"] += response.depression_score
            stats["insomnia_sum"] += response.insomnia_score
            stats["ocd_sum"] += response.ocd_score

        for stats in grouped.values():
            n = stats["n"] or 1
            stats["anxiety_mean"] = stats["anxiety_sum"] / n
            stats["depression_mean"] = stats["depression_sum"] / n
            stats["insomnia_mean"] = stats["insomnia_sum"] / n
            stats["ocd_mean"] = stats["ocd_sum"] / n
            for field in ("anxiety_sum", "depression_sum", "insomnia_sum", "ocd_sum"):
                stats.pop(field, None)
        return grouped

    @staticmethod
    def _hours_bucket(hours: float) -> str:
        """Map numeric hours into canonical buckets."""
        if hours <= 1:
            return "<=1"
        if hours <= 3:
            return "1-3"
        return ">3"
