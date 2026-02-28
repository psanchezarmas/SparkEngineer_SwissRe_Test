"""
NSE ETL Pipeline
Reads CLAIM_IDs from bronze claim data, fetches MD4 hashes from API, and stores in parquet table
"""
import logging
import os
import sys
from pathlib import Path
import yaml

os.environ.setdefault("SPARK_HOME", "/opt/spark")
spark_python_path = os.path.join(os.environ["SPARK_HOME"], "python")
if spark_python_path not in sys.path:
    sys.path.insert(0, spark_python_path)


from pyspark.sql import SparkSession
from api.nse_processor import NSEProcessor
from api.md4_hash_client import MD4HashClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def read_claim_ids(spark: SparkSession, claim_path: str) -> list:
    """
    Read CLAIM_IDs from bronze claim file.
    
    Args:
        spark: SparkSession instance
        claim_path: Path to the claim text/csv file
        
    Returns:
        List of unique CLAIM_IDs
    """
    logger.info(f"Reading claim file from: {claim_path}")
    
    # Read the text/csv file
    df = spark.read.option("header", "true").option("inferSchema", "true").text(claim_path)
    
    # If it's a CSV, read it properly
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(claim_path)
    
    # Extract CLAIM_ID column and collect as list
    claim_ids = df.select("CLAIM_ID").distinct().rdd.flatMap(lambda x: x).collect()
    
    logger.info(f"Found {len(claim_ids)} unique CLAIM_IDs")
    return claim_ids


def run_nse_pipeline(
    spark: SparkSession,
    claim_path: str,
    output_path: str,
    mode: str = "append"
) -> None:
    """
    Run the complete NSE pipeline: fetch API data and write to a parquet table.
    
    Args:
        spark: SparkSession instance
        claim_path: Path to bronze claim data
        output_path: Path to write bronze_nse_id parquet table
        mode: Write mode (overwrite, append)
    """
    logger.info("Starting NSE ETL Pipeline")
    
    # Read claim IDs
    claim_ids = read_claim_ids(spark, claim_path)
    
    if not claim_ids:
        logger.error("No CLAIM_IDs found. Exiting.")
        return

    # avoid re-processing claim_ids which already have NSE records
    # for this example, we check the existing parquet table for already-processed claim_ids and filter them out before API calls
    # in a production scenario we could have multiple alternatives:
    # - maintain a separate tracking table of processed claim_ids
    # - use a more robust upsert mechanism with Delta Lake or similar that would handle duplicates at write time
    # 
    existing_ids = set()
    nse_path = output_path
    try:
        if os.path.exists(nse_path):
            logger.info(f"Checking existing NSE data at {nse_path}")

            #error in delta
            existing_df = spark.read.format("delta").load(nse_path)
            #existing_df = spark.read.parquet(nse_path)
            existing_ids = {row['claim_id'] for row in existing_df.select('claim_id').collect()}
            logger.info(f"Found {len(existing_ids)} already-processed claim_ids")
    except Exception as e:
        logger.warning(f"Could not read existing NSE data, assuming none: {e}")

    # filter out duplicates
    claim_ids = [cid for cid in claim_ids if cid not in existing_ids]
    if not claim_ids:
        logger.info("All claim_ids already processed; nothing to do.")
        return
    
    # Initialize processor
    api_client = MD4HashClient()
    processor = NSEProcessor(spark, api_client)
    
    # Fetch and create DataFrame
    logger.info(f"Fetching MD4 hashes for {len(claim_ids)} claims")
    nse_df = processor.fetch_and_create_dataframe(claim_ids)
    
    if nse_df.count() == 0:
        logger.warning("No data fetched from API. Exiting.")
        return
    
    logger.info(f"Successfully created DataFrame with {nse_df.count()} records")
    
    # Write to bronze parquet table
    logger.info(f"Writing to delta table: {output_path}")
    processor.write_bronze_table(nse_df, output_path, mode=mode)
    
    logger.info("NSE ETL Pipeline completed successfully")


def main():
    """Main entry point for the NSE ETL pipeline."""
    # Initialize Spark
    # standard Spark session with local master

    spark = SparkSession.builder \
        .appName("NSE-API-Fetch") \
        .master("local[*]") \
        .config("spark.jars", "/opt/spark/jars/delta-spark_2.12-3.2.0.jar,/opt/spark/jars/delta-storage-3.2.0.jar") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.HDFSLogStore") \
        .getOrCreate()
    
    
    try:
        # Determine project root and load path configuration
        project_root = Path(__file__).parent.parent.parent
        config_file = project_root / "src" / "data" / "paths.yaml"
        logger.info(f"Loading path configuration from {config_file}")
        with open(config_file, "r") as f:
            paths = yaml.safe_load(f)

        # compute full paths
        claim_path = str(project_root / paths["bronze"]["claim"])
        output_path = str(project_root / paths["bronze"]["nse_id"])

        logger.info(f"Project root: {project_root}")
        logger.info(f"Claim path: {claim_path}")
        logger.info(f"Output path: {output_path}")

        # Run pipeline
        run_nse_pipeline(spark, claim_path, output_path)

    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
