"""Utility to read and display the NSE table produced by the ETL."""
import sys
from pathlib import Path

# ensure Spark is available
import os
os.environ.setdefault("SPARK_HOME", "/opt/spark")

from pyspark.sql import SparkSession


def main(path: str):
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Show-NSE") \
        .getOrCreate()

    df = spark.read.parquet(path)
    df.show(truncate=False)
    print(f"Total records: {df.count()}")

    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python show_nse.py <parquet-directory>")
        sys.exit()
    main(sys.argv[1])
