"""
Gold Transactions ETL Pipeline
Reads silver_transaction and produces gold_transaction in Parquet format.
"""

import logging
import os
import sys
from pathlib import Path
import yaml

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_date, year, month, dayofmonth

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_silver_table(spark: SparkSession, path: str) -> DataFrame:
    """
    Load the silver_transaction Delta table.
    """
    logger.info(f"Loading silver table from {path}")
    return spark.read.format("delta").load(path)


def transform_to_gold(silver_df: DataFrame) -> DataFrame:
    """
    Apply gold-layer transformations.
    For now, we just add the date in columns to create partition storing. 
    """
    logger.info("Transforming silver_transaction into gold_transaction")

    gold_df = (
        silver_df
        .withColumn("year", year(current_date()))
        .withColumn("month", month(current_date()))
        .withColumn("day", dayofmonth(current_date()))
    )

    return gold_df



def write_gold_parquet(df: DataFrame, path: str) -> None:
    try:
        logger.info(f"Writing gold Parquet to {path}")
        df.write.mode("overwrite").partitionBy("year", "month", "day").parquet(path)
        logger.info("Gold Parquet write completed")

    except Exception as e:
        logger.error(f"Failed writing gold Parquet: {e}", exc_info=True)
        raise



def main():
    """
    Main entry point for the Gold Transactions ETL pipeline.
    """
    spark = (
        SparkSession.builder
        .appName("Gold-Transactions-ETL")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


    try:
        project_root = Path(__file__).parent.parent.parent
        config_file = project_root / "src" / "data" / "paths.yaml"

        logger.info(f"Loading path configuration from {config_file}")
        with open(config_file, "r") as f:
            paths = yaml.safe_load(f)

        # Load silver table
        silver_path = str(project_root / paths["silver"]["transactions"])
        silver_df = load_silver_table(spark, silver_path)

        # Transform to gold
        gold_df = transform_to_gold(silver_df)

        # Write output as Parquet
        gold_path = str(project_root / paths["gold"]["transactions"])
        
        write_gold_parquet(gold_df, gold_path)

    except Exception as e:
        logger.error(f"Gold Transactions Pipeline failed: {str(e)}", exc_info=True)
        sys.exit()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
