"""Filter parsing helpers for analytics routes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


Boolean = bool | None


@dataclass(frozen=True)
class FilterCriteria:
    """Structured representation of user-specified filters."""

    age_group: str | None = None
    streaming_service: str | None = None
    favourite_genre: str | None = None
    music_effects: str | None = None
    while_working: Boolean = None
    exploratory: Boolean = None
    foreign_languages: Boolean = None
    instrumentalist: Boolean = None
    composer: Boolean = None
    hours_bucket: str | None = None

    @classmethod
    def from_request_args(cls, args: Mapping[str, str]) -> "FilterCriteria":
        """
        Build a FilterCriteria object from request args.

        Empty strings are normalised to None. Boolean fields accept a
        narrow vocabulary ("yes"/"no") to keep validation strict.
        """
        if not isinstance(args, Mapping):
            raise TypeError("args must be a mapping of str keys to str values")

        return cls(
            age_group=_normalise_str(args.get("age_group")),
            streaming_service=_normalise_str(args.get("streaming_service")),
            favourite_genre=_normalise_str(args.get("favourite_genre")),
            music_effects=_normalise_str(args.get("music_effects")),
            while_working=_parse_bool(args.get("while_working")),
            exploratory=_parse_bool(args.get("exploratory")),
            foreign_languages=_parse_bool(args.get("foreign_languages")),
            instrumentalist=_parse_bool(args.get("instrumentalist")),
            composer=_parse_bool(args.get("composer")),
            hours_bucket=_normalise_str(args.get("hours_bucket")),
        )


def _normalise_str(value: str | None) -> str | None:
    """Trim whitespace and convert empty strings to None."""
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def _parse_bool(value: str | None) -> Boolean:
    """Parse a yes/no string into a boolean."""
    if value is None:
        return None
    lowered = value.strip().lower()
    if lowered == "yes":
        return True
    if lowered == "no":
        return False
    if lowered == "":
        return None
    raise ValueError(f"Boolean filters must be 'yes' or 'no', got {value!r}")
