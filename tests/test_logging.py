import os
import unittest

from src.logging_utils import log_action  # type: ignore[import]


class TestLoggingDecorator(unittest.TestCase):
    def setUp(self) -> None:
        self.log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")
        os.makedirs(self.log_dir, exist_ok=True)
        self.log_path = os.path.join(self.log_dir, "test_app.log")

        if os.path.exists(self.log_path):
            os.remove(self.log_path)

    def _read_log_contents(self) -> str:
        if not os.path.exists(self.log_path):
            return ""
        with open(self.log_path, "r", encoding="utf-8") as log_file:
            return log_file.read()

    def test_log_action_writes_entry_on_function_call(self) -> None:
        """
        GIVEN a function decorated with @log_action
        WHEN the function is called
        THEN a log line should be appended to the log file, containing the action name.
        """
        from src.logging_utils import configure_logger  # type: ignore[import]

        configure_logger(self.log_path)

        @log_action("TEST_ACTION")
        def sample_function(x: int, y: int) -> int:
            return x + y

        result = sample_function(2, 3)
        self.assertEqual(5, result)

        contents = self._read_log_contents()
        self.assertIn("TEST_ACTION", contents)
        self.assertIn("sample_function", contents)

    def test_log_action_does_not_break_exceptions(self) -> None:
        """
        GIVEN a function that raises an exception
        WHEN decorated with @log_action and called
        THEN the original exception should still be propagated.
        """
        from src.logging_utils import configure_logger  # type: ignore[import]

        configure_logger(self.log_path)

        @log_action("FAILING_ACTION")
        def failing_function() -> None:
            raise ValueError("boom")

        with self.assertRaises(ValueError):
            failing_function()

        contents = self._read_log_contents()
        self.assertIn("FAILING_ACTION", contents)
