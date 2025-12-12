"""
Logging utilities for the Music & Mental Health Insights Tool.

Provides:
- configure_logger(log_path): configure and return a module-level logger.
- log_action(action_name): decorator that logs each call to the wrapped function.
"""

from __future__ import annotations

import logging
import os
from functools import wraps
from typing import Any, Callable, TypeVar, cast

F = TypeVar("F", bound=Callable[..., Any])

_LOGGER: logging.Logger | None = None


def configure_logger(log_path: str) -> logging.Logger:
    """
    Configure and return a logger that writes to the given log_path.

    """
    global _LOGGER

    logger = logging.getLogger("music_health_app")
    logger.setLevel(logging.INFO)

    # Remove existing handlers so we do not duplicate log lines when
    # tests reconfigure the logger with a different file.
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()

    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    handler = logging.FileHandler(log_path, encoding="utf-8")
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    _LOGGER = logger
    return logger


def log_action(action_name: str) -> Callable[[F], F]:
    """
    Decorator that logs each call to the wrapped function with the given action name.
    """

    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            logger = _LOGGER or logging.getLogger("music_health_app")
            logger.info("action=%s function=%s", action_name, func.__name__)
            return func(*args, **kwargs)

        return cast(F, wrapper)

    return decorator
