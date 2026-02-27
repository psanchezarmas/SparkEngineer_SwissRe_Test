"""
Entrypoint for running the Spark application from Docker or spark-submit.
"""

from __future__ import annotations
import sys
import os


def ensure_project_root_on_path():
    # /app/src/main.py → /app/src
    src_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(src_dir)  # /app

    # Add /app/src to PYTHONPATH
    if src_dir not in sys.path:
        sys.path.insert(0, src_dir)

    # Add /app to PYTHONPATH (optional but safe)
    if project_root not in sys.path:
        sys.path.insert(0, project_root)


def main():
    ensure_project_root_on_path()

    # Import AFTER fixing sys.path
    from spark_app.main import main as spark_main

    # Forward CLI args exactly as spark-submit gives them
    return spark_main()


if __name__ == "__main__":
    sys.exit(main())
