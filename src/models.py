"""
Domain models and analysis logic for the Music & Mental Health Insights Tool.

At this stage we only define minimal stubs so that unit tests can import them.
We will implement the real logic step by step using TDD.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional


def map_frequency_to_numeric(label: str) -> int:
    """
    Stub implementation.

    Real implementation will convert labels like "Rarely" or
    "Very frequently" into integer codes. For now, we raise
    NotImplementedError so tests can drive the behaviour.
    """
    raise NotImplementedError("map_frequency_to_numeric is not implemented yet")


@dataclass
class SurveyResponse:
    """
    Stub representation of a single survey response.

    Real implementation will include validation and a genre_frequencies
    dictionary. For now, we only define the attributes used by tests.
    """

    timestamp: str
    age: int
    primary_streaming_service: str
    hours_per_day: float
    while_working: bool
    instrumentalist: bool
    composer: bool
    fav_genre: str
    exploratory: bool
    foreign_languages: bool
    bpm: Optional[int]
    anxiety_score: int
    depression_score: int
    insomnia_score: int
    ocd_score: int
    music_effects: str
    genre_frequencies: Dict[str, int]

    # No __post_init__ yet – tests will drive what we need.


@dataclass
class AnalysisEngine:
    """
    Stub analysis engine.

    Real implementation will compute averages, counts, and other
    analytics over a list of SurveyResponse objects.
    """

    responses: List[SurveyResponse]

    def get_average_anxiety_by_genre(self, genre: str) -> Optional[float]:
        raise NotImplementedError

    def get_average_depression_by_genre(self, genre: str) -> Optional[float]:
        raise NotImplementedError

    def get_count_by_streaming_service(self) -> Dict[str, int]:
        raise NotImplementedError

    def get_users_with_insomnia_score(self, threshold: int) -> List[SurveyResponse]:
        raise NotImplementedError

    def get_hours_per_day_vs_average_anxiety(self) -> Dict[str, float]:
        raise NotImplementedError
