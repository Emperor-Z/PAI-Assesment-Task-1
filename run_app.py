"""
Development entry point for the Flask web application.

Run with:
    python run_app.py
"""

from __future__ import annotations

from src.app import create_app  # type: ignore[import]


def main() -> None:
    """Create the Flask application and run the development server."""
    app = create_app(testing=False)
    app.run(
        host="127.0.0.1",
        port=5000,
        debug=True,
        use_reloader=False,
    )


if __name__ == "__main__":
    main()
