"""
NSE (Non-Standardized Entity) Processor
Processes CLAIM_IDs to fetch NSE identifiers via MD4 hash API and create parquet tables
"""
import logging
from typing import List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from datetime import datetime
from .md4_hash_client import MD4HashClient

logger = logging.getLogger(__name__)


class NSEProcessor:
    """Processes NSE data from API and creates parquet tables."""
    
    def __init__(self, spark: SparkSession, api_client: MD4HashClient = None):
        """
        Initialize NSE Processor.
        
        Args:
            spark: SparkSession instance
            api_client: MD4HashClient instance (creates new if None)
        """
        self.spark = spark
        self.api_client = api_client or MD4HashClient()
    
    def get_schema(self) -> StructType:
        """
        Get the schema for bronze_nse_id table.
        
        Returns:
            StructType schema definition
        """
        return StructType([
            StructField("claim_id", StringType(), False),
            StructField("digest", StringType(), False),
            StructField("digest_enc", StringType(), True),
            StructField("type", StringType(), True),
            StructField("key", StringType(), True),
            StructField("stored_timestamp", TimestampType(), False),
            StructField("processed_date", TimestampType(), False),
        ])
    
    def fetch_and_create_dataframe(self, claim_ids: List[str]) -> DataFrame:
        """
        Fetch NSE IDs from API and create a Spark DataFrame.
        
        Args:
            claim_ids: List of CLAIM_IDs to process
            
        Returns:
            Spark DataFrame with NSE data
        """
        logger.info(f"Fetching NSE IDs for {len(claim_ids)} claims")
        
        # Fetch data from API
        api_results = self.api_client.fetch_hashes_batch(claim_ids)
        
        if not api_results:
            logger.warning("No results from API")
            return self.spark.createDataFrame([], self.get_schema())
        
        # Process results
        processed_data = []
        for result in api_results:
            # convert timestamp strings to actual datetime objects
            stored_ts = result.get('stored_timestamp')
            if isinstance(stored_ts, str):
                try:
                    stored_ts = datetime.fromisoformat(stored_ts)
                except ValueError:
                    # fallback: leave as string, Spark will attempt to cast
                    pass

            proc_date = datetime.utcnow()

            processed_data.append({
                'claim_id': result.get('claim_id'),
                'digest': result.get('Digest'),
                'digest_enc': result.get('DigestEnc'),
                'type': result.get('Type'),
                'key': result.get('Key'),
                'stored_timestamp': stored_ts,
                'processed_date': proc_date,
            })
        
        # Create DataFrame
        df = self.spark.createDataFrame(processed_data, schema=self.get_schema())
        logger.info(f"Created DataFrame with {df.count()} records")
        
        return df
    
    def write_bronze_table(self, df: DataFrame, path: str, mode: str = "append") -> None:
        """
        Write DataFrame to bronze storage as Parquet.
        
        Args:
            df: DataFrame to write
            path: Path to write the table (directory)
            mode: Write mode (append, overwrite, etc.)
        """
        try:
            df.write.mode(mode).parquet(path)
            logger.info(f"Successfully wrote {df.count()} records to {path}")
        except Exception as e:
            logger.error(f"Failed to write table: {str(e)}")
            raise
