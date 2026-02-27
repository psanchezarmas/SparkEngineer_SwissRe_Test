"""PyTest fixtures for the Spark application tests."""
from __future__ import annotations

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session() -> SparkSession:
    """Create a SparkSession for tests.

    The session is shared across all tests in the session to speed up execution.
    """
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("spark_app_test")
        .getOrCreate()
    )
    yield spark
    spark.stop()
