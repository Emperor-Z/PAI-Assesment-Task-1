"""
Service layer for the Music & Mental Health Insights Tool.

InsightsService wraps the AnalysisEngine to provide higher-level
operations that will later be used by controllers or Flask routes.
All public methods are decorated with log_action so that each call is
captured in the application log.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

from src.logging_utils import log_action
from src.models import AnalysisEngine, SurveyResponse


@dataclass
class InsightsService:
    responses: List[SurveyResponse]

    def __post_init__(self) -> None:
        self._engine = AnalysisEngine(self.responses)

    @log_action("GET_GENRE_MENTAL_HEALTH_STATS")
    def get_average_anxiety_and_depression_by_genre(self, genre: str) -> Dict[str, float]:
        avg_anxiety = self._engine.get_average_anxiety_by_genre(genre) or 0.0
        avg_depression = self._engine.get_average_depression_by_genre(genre) or 0.0
        return {
            "genre": genre,
            "avg_anxiety": avg_anxiety,
            "avg_depression": avg_depression,
        }

    @log_action("GET_STREAMING_COUNTS")
    def get_streaming_service_counts(self) -> Dict[str, int]:
        return self._engine.get_count_by_streaming_service()

    @log_action("GET_HOURS_VS_ANXIETY")
    def get_hours_vs_anxiety(self) -> Dict[str, float]:
        return self._engine.get_hours_per_day_vs_average_anxiety()
