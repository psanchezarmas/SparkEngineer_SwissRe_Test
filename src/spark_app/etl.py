"""ETL utilities for the Spark application."""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, avg


def load_data(spark: SparkSession, path: str) -> DataFrame:
    """Read data from a CSV file into a DataFrame.

    The CSV is expected to have a header and infer schema.

    Args:
        spark: active SparkSession.
        path: path to the input CSV.

    Returns:
        A ``DataFrame`` containing the data.
    """
    return (
        spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    )


def transform_data(df: DataFrame) -> DataFrame:
    """Perform transformation on the input DataFrame.

    For demonstration purposes, compute the average ``value`` per ``category``.
    Any columns other than ``category`` and ``value`` are passed through.

    Args:
        df: input DataFrame with at least ``category`` (string) and ``value`` (numeric).

    Returns:
        Transformed DataFrame with columns ``category`` and ``avg_value``.
    """
    # validate expected columns exist
    required = {"category", "value"}
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    return (
        df.groupBy("category").agg(avg(col("value")).alias("avg_value"))
    )


def save_data(df: DataFrame, path: str) -> None:
    """Write the DataFrame to the given path as parquet.

    Args:
        df: DataFrame to write.
        path: destination directory.
    """
    df.write.mode("overwrite").parquet(path)


def run(spark: SparkSession, input_path: str, output_path: str) -> None:
    """Run full ETL: load, transform, and save.

    Args:
        spark: SparkSession to use.
        input_path: path to source CSV file.
        output_path: directory where results are written.
    """
    df = load_data(spark, input_path)
    transformed = transform_data(df)
    save_data(transformed, output_path)
