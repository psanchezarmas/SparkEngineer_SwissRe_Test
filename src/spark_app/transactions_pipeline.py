"""
Silver Transactions ETL Pipeline
Reads bronze_claim, bronze_contract, bronze_nse_id and produces silver_transaction
"""

import logging
import os
import sys
from pathlib import Path
import yaml
from delta.tables import DeltaTable

os.environ.setdefault("SPARK_HOME", "/opt/spark")
spark_python_path = os.path.join(os.environ["SPARK_HOME"], "python")
if spark_python_path not in sys.path:
    sys.path.insert(0, spark_python_path)

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, regexp_replace, when, date_format, current_timestamp, lit, to_date, to_timestamp
)


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_bronze_tables(spark: SparkSession, paths: dict):
    """
    Load bronze claim, contract, and nse_id tables from Delta or CSV/TXT.
    """
    logger.info("Loading bronze tables")

    claim_path = paths["bronze"]["claim"]
    contract_path = paths["bronze"]["contract"]
    nse_path = paths["bronze"]["nse_id"]

    # bronze_claim and bronze_contract are TXT/CSV files
    claim_df = spark.read.option("header", "true").option("inferSchema", "true").csv(claim_path)
    contract_df = spark.read.option("header", "true").option("inferSchema", "true").csv(contract_path)

    # bronze_nse_id is a Delta table
    nse_df = spark.read.format("delta").load(nse_path)

    return claim_df, contract_df, nse_df

    # The transformation logic to create the silver_transaction table
    
def transform_to_silver(claim_df, contract_df, nse_df):
    logger.info("Transforming bronze tables into silver_transaction")

    contract_df = contract_df.drop("CREATION_DATE") # Drop of the column due to duplicated name and error when selecting column

    joined_df = (
        claim_df.alias("c")
        .join(
            contract_df.alias("ct"),
            (col("c.CONTRACT_SOURCE_SYSTEM") == col("ct.SOURCE_SYSTEM")) & #assuring contracts come from same system
            (col("c.CONTRACT_ID") == col("ct.CONTRACT_ID")),
            "left"
        )
        .join(
            nse_df.alias("n"),
            col("c.CLAIM_ID") == col("n.claim_id"),
            "left"
        )
    )
    # Query to create silver_transaction with the required transformations and mappings
    # Same as schema
    silver_df = (
        joined_df
        .withColumn("CONTRACT_SOURCE_SYSTEM", lit("Europe 3"))
        .withColumn("CONTRACT_SOURCE_SYSTEM_ID", col("ct.CONTRACT_ID"))
        .withColumn("SOURCE_SYSTEM_ID", regexp_replace(col("c.CLAIM_ID"), "^[A-Za-z_]+", ""))
        .withColumn(
            "TRANSACTION_TYPE",
            when(col("c.CLAIM_TYPE") == 2, "Corporate")
            .when(col("c.CLAIM_TYPE") == 1, "Private")
            .otherwise("Unknown")
        )
        .withColumn(
            "TRANSACTION_DIRECTION",
            when(col("c.CLAIM_ID").like("%CL%"), "COINSURANCE")
            .when(col("c.CLAIM_ID").like("%RX%"), "REINSURANCE")
        )
        .withColumn("CONFORMED_VALUE", col("c.AMOUNT"))
        .withColumn( "BUSINESS_DATE", date_format(to_date(col("c.DATE_OF_LOSS"), "dd.MM.yyyy"), "yyyy-MM-dd") ) # To adapt to the current format of source
        .withColumn( "CREATION_DATE", date_format(to_timestamp(col("c.CREATION_DATE"), "dd.MM.yyyy HH:mm"), "yyyy-MM-dd HH:mm:ss") ) # To adapt to the current format of source
        .withColumn("SYSTEM_TIMESTAMP", current_timestamp())
        .withColumn("NSE_ID", col("n.digest"))
    )

    silver_df = silver_df.select(
        "CONTRACT_SOURCE_SYSTEM",
        "CONTRACT_SOURCE_SYSTEM_ID",
        "SOURCE_SYSTEM_ID",
        "TRANSACTION_TYPE",
        "TRANSACTION_DIRECTION",
        "CONFORMED_VALUE",
        "BUSINESS_DATE",
        "CREATION_DATE",
        "SYSTEM_TIMESTAMP",
        "NSE_ID"
    )
    # logger.info("Show silver_df:") 
    # logger.info(silver_df._jdf.showString(20, 20, False))
    # To debug the output of the transformation, we can show the first few rows of the silver_df DataFrame.
    return silver_df




def write_silver_table(spark: SparkSession, df: DataFrame, path: str) -> None:
    """
    Write the silver_transaction table using an UPSERT (merge) operation.
    Ensures no duplicates are created. A record is considered the same when both
    SOURCE_SYSTEM_ID and CONTRACT_SOURCE_SYSTEM_ID match.
    """
    try:
        if DeltaTable.isDeltaTable(spark, path):
            delta_table = DeltaTable.forPath(spark, path)

            (
                    delta_table.alias("t")
                    .merge(df.alias("s"), """t.SOURCE_SYSTEM_ID = s.SOURCE_SYSTEM_ID
                    AND (
                        (t.CONTRACT_SOURCE_SYSTEM_ID = s.CONTRACT_SOURCE_SYSTEM_ID)
                        OR (t.CONTRACT_SOURCE_SYSTEM_ID IS NULL AND s.CONTRACT_SOURCE_SYSTEM_ID IS NULL)
                    )
                    """) # Due to troubles to manage the nulls we need to do this condition
                    .whenNotMatchedInsertAll()
                    .execute()
                )


            logger.info(f"Upsert completed in {path}")


        else:
            df.write.format("delta").mode("overwrite").save(path)
            logger.info(f"Delta table created in {path}")

    except Exception as e:
        logger.error(f"Failed writing silver table: {e}", exc_info=True)
        raise


def main():
    """
    Main entry point for the Silver Transactions ETL pipeline.
    """
    spark = SparkSession.builder \
        .appName("Silver-Transactions-ETL")  \
        .master("local[*]") \
        .config("spark.jars", "/opt/spark/jars/delta-spark_2.12-3.2.0.jar,/opt/spark/jars/delta-storage-3.2.0.jar") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.HDFSLogStore") \
        .getOrCreate()
    

    try:
        project_root = Path(__file__).parent.parent.parent
        config_file = project_root / "src" / "data" / "paths.yaml"

        logger.info(f"Loading path configuration from {config_file}")
        with open(config_file, "r") as f:
            paths = yaml.safe_load(f)

        # Load bronze tables
        claim_df, contract_df, nse_df = load_bronze_tables(spark, paths)

        # Transform to silver
        silver_df = transform_to_silver(claim_df, contract_df, nse_df)

        # Write output
        output_path = str(project_root / paths["silver"]["transactions"])
        write_silver_table(spark, silver_df, output_path)

    except Exception as e:
        logger.error(f"Silver Transactions Pipeline failed: {str(e)}", exc_info=True)
        sys.exit()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
