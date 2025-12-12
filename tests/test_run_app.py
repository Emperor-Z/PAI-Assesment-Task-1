"""Tests for the run_app module."""

import importlib
import unittest
from types import ModuleType


class RunAppImportTests(unittest.TestCase):
    """Test importing run_app."""

    def test_run_app_importable(self) -> None:
        """Ensure that run_app module can be imported."""
        module: ModuleType = importlib.import_module("run_app")
        self.assertIsNotNone(module)


class RunAppMainCallableTests(unittest.TestCase):
    """Test entrypoint exposure."""

    def test_run_app_has_main_callable(self) -> None:
        """Verify run_app exposes a callable entrypoint."""
        module: ModuleType = importlib.import_module("run_app")
        self.assertTrue(hasattr(module, "main"))
        self.assertTrue(callable(module.main))
