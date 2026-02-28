"""
NSE (Non-Standardized Entity) Processor
Processes CLAIM_IDs to fetch NSE identifiers via MD4 hash API and create parquet tables
"""
import logging
from typing import List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import ( StructType, StructField, StringType, TimestampType, LongType, DateType, DecimalType ) 
from datetime import datetime
from .md4_hash_client import MD4HashClient
from delta.tables import DeltaTable
import yaml 
import os


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
        Load schema for bronze_nse_id from schemas.yaml dynamically.
        """

        # Build absolute path to schemas.yaml 
        # Assuming this code runs in a container where /app is the root of the project, and schemas.yaml is at src/data/schemas.yaml
        
        yaml_path = "/app/src/data/schemas.yaml"
        if not os.path.exists(yaml_path):
            raise FileNotFoundError(f"schemas.yaml not found: {yaml_path}")
        

        # Load YAML
        with open(yaml_path, "r") as f:
            config = yaml.safe_load(f)

        # Find the bronze_nse_id table
        tables = config.get("tables", [])
        bronze_tables = next((t for t in tables if t.get("layer") == "bronze"), None)

        if not bronze_tables:
            raise ValueError("No bronze layer found in schemas.yaml")

        nse_table = next(
            (tbl for tbl in bronze_tables.get("tables", [])
            if tbl.get("table_name") == "bronze_nse_id"),
            None
        )

        if not nse_table:
            raise ValueError("bronze_nse_id not found in schemas.yaml")

        # Map YAML types → Spark types
        # Add all the data types you expect to use in your schemas here
        type_map = {
            "string": StringType(),
            "timestamp": TimestampType(),
            "date": DateType(),
            "long": LongType(),
            
        }

        def parse_type(type_str: str):
            if type_str.startswith("decimal"):
                # decimal(16,5)
                inside = type_str[type_str.find("(") + 1:type_str.find(")")]
                precision, scale = map(int, inside.split(","))
                return DecimalType(precision, scale)
            return type_map.get(type_str)

        # Build StructType
        fields = []
        for col in nse_table["columns"]:
            spark_type = parse_type(col["type"])
            if spark_type is None:
                raise ValueError(f"Unsupported type in YAML: {col['type']}")

            fields.append(
                StructField(
                    col["name"],
                    spark_type,
                    col.get("nullable", True)
                )
            )
        return StructType(fields)

    
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


    def write_bronze_table(self, df: DataFrame, path: str) -> None:
        """
        Upsert en Delta: if claim_id exists, it does not insert the dupicate, otherwise it inserts the new record.
        """
        try:
            if DeltaTable.isDeltaTable(self.spark, path):
                delta_table = DeltaTable.forPath(self.spark, path)

                (
                    delta_table.alias("t")
                    .merge(
                        df.alias("s"),
                        "t.claim_id = s.claim_id"
                    )
                    .whenNotMatchedInsertAll()
                    .execute()
                )
                logger.info(f"Upsert completed in {path}")
            else:
                df.write.format("delta").mode("overwrite").save(path)
                logger.info(f"Delta table created in {path}")

        except Exception as e:
            logger.error(f"Error in MERGE: {str(e)}")
            raise

