"""
Domain models and analysis logic for the Music & Mental Health Insights Tool.

At this stage we only define minimal stubs so that unit tests can import them.
We will implement the real logic step by step using TDD.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional


from dataclasses import dataclass
from typing import Dict, List, Optional


# --- Frequency mapping helper ------------------------------------------------

FREQUENCY_MAPPING: Dict[str, int] = {
    "never": 0,
    "rarely": 1,
    "sometimes": 2,
    "very frequently": 3,
}


def map_frequency_to_numeric(label: str) -> int:
    """
    Convert a textual listening frequency label into a numeric score.

    Mapping:
    - "Never"           -> 0
    - "Rarely"          -> 1
    - "Sometimes"       -> 2
    - "Very frequently" -> 3

    Behaviour:
    - Ignores leading/trailing spaces.
    - Case-insensitive.
    - Fails fast on unknown or empty labels.

    Raises
    ------
    TypeError
        If label is not a string.
    ValueError
        If label is empty or not recognised.
    """
    # Guard clause: wrong type
    if not isinstance(label, str):
        raise TypeError(f"label must be a str, got {type(label).__name__}")

    # Normalise: strip spaces and lowercase
    normalised = label.strip().lower()
    if not normalised:
        # Guard clause: empty after stripping
        raise ValueError("Frequency label must not be empty")

    # Look up in mapping table
    if normalised not in FREQUENCY_MAPPING:
        # Fail fast on unknown values to make data issues visible
        raise ValueError(f"Unknown frequency label: {label!r}")

    return FREQUENCY_MAPPING[normalised]



from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass
class SurveyResponse:
    """
    Cleaned, typed representation of a single survey response.

    This class is the core domain entity for our tool. It assumes that
    the incoming data has already been cleaned to a reasonable level,
    but still enforces type checks as a guard against programming errors.

    Attributes (summary):
    - timestamp: raw timestamp string.
    - age: int (years).
    - primary_streaming_service: main streaming platform.
    - hours_per_day: float (listening hours per day).
    - while_working / instrumentalist / composer / exploratory /
      foreign_languages: booleans.
    - bpm: int or None.
    - anxiety_score / depression_score / insomnia_score / ocd_score: ints.
    - music_effects: short free-text description.
    - genre_frequencies: dict[genre_name -> frequency_score].
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

    def __post_init__(self) -> None:
        """
        Validate types and normalise fields after initialisation.

        This method enforces the expectations captured in the unit tests.
        It follows a 'guard clause' style: we fail early with clear
        error messages if something looks wrong, rather than allowing
        inconsistent state into the system.
        """
        # --- Scalar type checks ------------------------------------------------
        if not isinstance(self.age, int):
            raise TypeError("age must be an int")

        if not isinstance(self.primary_streaming_service, str):
            raise TypeError("primary_streaming_service must be a str")

        if not isinstance(self.hours_per_day, (int, float)):
            raise TypeError("hours_per_day must be a number")
        # Normalise hours_per_day to float internally
        self.hours_per_day = float(self.hours_per_day)

        # --- Boolean fields ----------------------------------------------------
        for attr_name in (
            "while_working",
            "instrumentalist",
            "composer",
            "exploratory",
            "foreign_languages",
        ):
            value = getattr(self, attr_name)
            if not isinstance(value, bool):
                raise TypeError(f"{attr_name} must be a bool")

        # --- Mental health scores (0–10) --------------------------------------
        for attr_name in (
            "anxiety_score",
            "depression_score",
            "insomnia_score",
            "ocd_score",
        ):
            value = getattr(self, attr_name)
            if not isinstance(value, int):
                raise TypeError(f"{attr_name} must be an int")

        # --- Optional BPM ------------------------------------------------------
        if self.bpm is not None and not isinstance(self.bpm, int):
            raise TypeError("bpm must be an int or None")

        # --- String fields -----------------------------------------------------
        if not isinstance(self.fav_genre, str):
            raise TypeError("fav_genre must be a str")

        if not isinstance(self.music_effects, str):
            raise TypeError("music_effects must be a str")

        # --- Genre frequencies -------------------------------------------------
        if not isinstance(self.genre_frequencies, dict):
            raise TypeError("genre_frequencies must be a dict[str, int]")

        for genre, freq in self.genre_frequencies.items():
            if not isinstance(genre, str):
                raise TypeError("genre_frequencies keys must be str")
            if not isinstance(freq, int):
                raise TypeError(
                    f"genre_frequencies[{genre!r}] must be an int"
                )


@dataclass
class AnalysisEngine:
    """
    Stub analysis engine.

    Real implementation will compute averages, counts, and other
    analytics over a list of SurveyResponse objects.
    """

    responses: List[SurveyResponse]

    def get_average_anxiety_by_genre(self, genre: str) -> Optional[float]:
        """
        Calculate the average anxiety score for respondents whose
        favourite genre matches the given genre (case-insensitive).

        Returns
        -------
        Optional[float]
            The arithmetic mean anxiety score, or None if there are no matches.

        This behaviour is exactly what the unit tests expect:
        - "Pop" with one matching respondent → 3.0
        - "Country" with no matches       → None
        """
        # Guard clause: wrong type
        if not isinstance(genre, str):
            raise TypeError("genre must be a str")

        # Normalise for case-insensitive comparison
        genre_lower = genre.strip().lower()
        if not genre_lower:
            # If someone passes an empty string, treat as "no results"
            return None

        # Collect anxiety scores for matching favourite genre
        scores: List[int] = [
            response.anxiety_score
            for response in self.responses
            if response.fav_genre.strip().lower() == genre_lower
        ]

        if not scores:
            return None

        return sum(scores) / len(scores)


     def get_average_depression_by_genre(self, genre: str) -> Optional[float]:
        """
        Calculate the average depression score for respondents whose
        favourite genre matches the given genre (case-insensitive).

        Returns None if there are no matching respondents.

        Mirrors the logic for anxiety, but uses depression_score.
        """
        if not isinstance(genre, str):
            raise TypeError("genre must be a str")

        genre_lower = genre.strip().lower()
        if not genre_lower:
            return None

        scores: List[int] = [
            response.depression_score
            for response in self.responses
            if response.fav_genre.strip().lower() == genre_lower
        ]

        if not scores:
            return None

        return sum(scores) / len(scores)


    def get_count_by_streaming_service(self) -> Dict[str, int]:
        """
        Count how many respondents use each primary streaming service.

        Returns
        -------
        Dict[str, int]
            Mapping from service name (e.g. "Spotify") to count.

        Notes
        -----
        This implementation runs in O(n) time for n responses. If needed
        for performance, it could later be refactored to use an internal
        cache for O(1) average-time lookups, but simplicity is preferred
        until profiling indicates otherwise.
        """
        counts: Dict[str, int] = {}
        for response in self.responses:
            service = response.primary_streaming_service
            counts[service] = counts.get(service, 0) + 1
        return counts

    def get_users_with_insomnia_score(self, threshold: int) -> List[SurveyResponse]:
        raise NotImplementedError

    def get_hours_per_day_vs_average_anxiety(self) -> Dict[str, float]:
        raise NotImplementedError
