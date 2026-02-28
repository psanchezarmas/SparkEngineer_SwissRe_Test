# nse_processor.py

import logging
from typing import List
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from delta.tables import DeltaTable

from .md4_hash_client import MD4HashClient
from .utils import load_yaml_schema, build_schema_from_yaml

logger = logging.getLogger(__name__)


class NSEProcessor:
    """Processes NSE data from API and creates parquet tables."""

    def __init__(self, spark: SparkSession, api_client: MD4HashClient = None):
        self.spark = spark
        self.api_client = api_client or MD4HashClient()

    def get_schema(self):
        yaml_path = "/app/src/data/schemas.yaml"
        config = load_yaml_schema(yaml_path)
        return build_schema_from_yaml(config, "bronze_nse_id")

    def fetch_and_create_dataframe(self, claim_ids: List[str]) -> DataFrame:
        logger.info(f"Fetching NSE IDs for {len(claim_ids)} claims")

        api_results = self.api_client.fetch_hashes_batch(claim_ids)

        if not api_results:
            logger.warning("No results from API")
            return self.spark.createDataFrame([], self.get_schema())

        processed_data = []
        for result in api_results:
            stored_ts = result.get("stored_timestamp")
            if isinstance(stored_ts, str):
                try:
                    stored_ts = datetime.fromisoformat(stored_ts)
                except ValueError:
                    pass

            processed_data.append({
                "claim_id": result.get("claim_id"),
                "digest": result.get("Digest"),
                "digest_enc": result.get("DigestEnc"),
                "type": result.get("Type"),
                "key": result.get("Key"),
                "stored_timestamp": stored_ts,
                "processed_date": datetime.utcnow(),
            })

        df = self.spark.createDataFrame(processed_data, schema=self.get_schema())
        logger.info(f"Created DataFrame with {df.count()} records")

        return df

    def write_bronze_table(self, df: DataFrame, path: str) -> None:
        try:
            if DeltaTable.isDeltaTable(self.spark, path):
                delta_table = DeltaTable.forPath(self.spark, path)

                (
                    delta_table.alias("t")
                    .merge(df.alias("s"), "t.claim_id = s.claim_id")
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
