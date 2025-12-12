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
from src.filters import FilterCriteria
from src.logging_utils import log_action
from src.models import AnalysisEngine, SurveyResponse


@dataclass
class InsightsService:
    """Provide analytics-friendly operations backed by curated DB data."""

    db_manager: DatabaseManager
    _responses_cache: List[SurveyResponse] | None = field(default=None, init=False)
    _engine: AnalysisEngine | None = field(default=None, init=False)

    def _get_responses(self) -> List[SurveyResponse]:
        """Lazy-load SurveyResponse objects from the database."""
        if self._responses_cache is None:
            self._responses_cache = self.db_manager.get_all_clean_responses()
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
            return self.db_manager.get_clean_responses_filtered(criteria)
        return self._get_responses()

    @staticmethod
    def _criteria_has_filters(criteria: FilterCriteria) -> bool:
        """Determine whether any filter fields are active."""
        return any(value is not None for value in criteria.__dict__.values())

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
