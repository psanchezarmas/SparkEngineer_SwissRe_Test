"""Command‑line entry point for the Spark ETL application."""
from __future__ import annotations

import argparse
import logging
import sys
from pyspark.sql import SparkSession

from . import etl


def create_spark_session(app_name: str = "spark_app") -> SparkSession:
    """Build a SparkSession configured for local development.

    Args:
        app_name: name to give the Spark application.

    Returns:
        a ``SparkSession`` instance.
    """
    return (
        SparkSession.builder.appName(app_name).master("local[*]")
        .getOrCreate()
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run ETL job on input CSV")
    parser.add_argument("--input", required=True, help="Path to input CSV file")
    parser.add_argument("--output", required=True, help="Path to output directory")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Application entrypoint.

    Returns 0 on success, non-zero on failure.
    """
    logging.basicConfig(level=logging.INFO)
    args = parse_args(argv)

    spark = create_spark_session()
    try:
        etl.run(spark, args.input, args.output)
        logging.info("ETL job finished successfully")
        return 0
    except Exception as e:  # pragma: no cover - let tests exercise failure cases
        logging.exception("ETL job failed")
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
