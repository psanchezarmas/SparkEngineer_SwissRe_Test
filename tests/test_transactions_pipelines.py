# tests/test_transactions_pipeline.py

from unittest.mock import patch, MagicMock, mock_open

from spark_app.transactions_pipeline import (
    transform_to_silver,
    write_silver_table,
    load_bronze_tables,
)
import spark_app.transactions_pipeline as transactions_pipeline
from pyspark.sql.readwriter import DataFrameReader


# --------------------------------------------------------- 
# Test: Basic transformation from bronze → silver 
# Ensures correct mappings, joins, derived fields, and NSE digest merge 
# ---------------------------------------------------------
def test_transform_to_silver_basic(spark):
    claim_df = spark.createDataFrame([
        ("CL123", 1, "SYS1", "C1", 100.0, "2024-01-01", "2024-01-02")
    ], ["CLAIM_ID", "CLAIM_TYPE", "CONTRACT_SOURCE_SYSTEM", "CONTRACT_ID",
        "AMOUNT", "DATE_OF_LOSS", "CREATION_DATE"])

    contract_df = spark.createDataFrame([
        ("SYS1", "C1", "2023-01-01")
    ], ["SOURCE_SYSTEM", "CONTRACT_ID", "CREATION_DATE"])

    nse_df = spark.createDataFrame([
        ("CL123", "digest123")
    ], ["claim_id", "digest"])

    silver = transform_to_silver(claim_df, contract_df, nse_df)
    row = silver.collect()[0]

    assert row.CONTRACT_SOURCE_SYSTEM == "Europe 3"
    assert row.CONTRACT_SOURCE_SYSTEM_ID == "C1"
    assert row.SOURCE_SYSTEM_ID == "123"  # CL123 → 123
    assert row.TRANSACTION_TYPE == "Private"
    assert row.TRANSACTION_DIRECTION == "COINSURANCE"
    assert row.CONFORMED_VALUE == 100.0
    assert row.BUSINESS_DATE == "2024-01-01"
    assert row.CREATION_DATE.startswith("2024-01-02")
    assert row.NSE_ID == "digest123"

# --------------------------------------------------------- 
# Test: Unknown claim type should map to default values 
# ---------------------------------------------------------
def test_transform_to_silver_unknown_type(spark):
    claim_df = spark.createDataFrame([
        ("RX999", 99, "SYS1", "C1", 50.0, "2024-02-01", "2024-02-02")
    ], ["CLAIM_ID", "CLAIM_TYPE", "CONTRACT_SOURCE_SYSTEM", "CONTRACT_ID",
        "AMOUNT", "DATE_OF_LOSS", "CREATION_DATE"])

    contract_df = spark.createDataFrame([
        ("SYS1", "C1", "2023-01-01")
    ], ["SOURCE_SYSTEM", "CONTRACT_ID", "CREATION_DATE"])

    nse_df = spark.createDataFrame([], "claim_id STRING, digest STRING")

    silver = transform_to_silver(claim_df, contract_df, nse_df)
    row = silver.collect()[0]

    assert row.TRANSACTION_TYPE == "Unknown"
    assert row.TRANSACTION_DIRECTION == "REINSURANCE"

# --------------------------------------------------------- 
# Test: Missing contract join should produce null contract fields 
# ---------------------------------------------------------
def test_transform_to_silver_missing_contract(spark):
    claim_df = spark.createDataFrame([
        ("CL123", 1, "SYSX", "C999", 100.0, "2024-01-01", "2024-01-02")
    ], ["CLAIM_ID", "CLAIM_TYPE", "CONTRACT_SOURCE_SYSTEM", "CONTRACT_ID",
        "AMOUNT", "DATE_OF_LOSS", "CREATION_DATE"])

    contract_df = spark.createDataFrame([], "SOURCE_SYSTEM STRING, CONTRACT_ID STRING, CREATION_DATE STRING")

    nse_df = spark.createDataFrame([], "claim_id STRING, digest STRING")

    silver = transform_to_silver(claim_df, contract_df, nse_df)
    row = silver.collect()[0]

    assert row.CONTRACT_SOURCE_SYSTEM_ID is None


# --------------------------------------------------------- 
# Test: write_silver_table performs Delta merge when table exists 
# ---------------------------------------------------------
def test_write_silver_table_merge(spark):
    df = spark.createDataFrame([(1, 2)], ["SOURCE_SYSTEM_ID", "CONTRACT_SOURCE_SYSTEM_ID"])

    with patch("spark_app.transactions_pipeline.DeltaTable.isDeltaTable", return_value=True), \
         patch("spark_app.transactions_pipeline.DeltaTable.forPath") as mock_forpath:

        mock_table = MagicMock()
        mock_forpath.return_value = mock_table


        mock_alias = MagicMock()
        mock_table.alias.return_value = mock_alias


        mock_merge = MagicMock()
        mock_alias.merge.return_value = mock_merge


        mock_merge.whenNotMatchedInsertAll.return_value = mock_merge

        write_silver_table(spark, df, "/fake/path")

        mock_table.alias.assert_called_once_with("t")
        mock_alias.merge.assert_called_once()
        mock_merge.whenNotMatchedInsertAll.assert_called_once()
        mock_merge.execute.assert_called_once()


# --------------------------------------------------------- 
# Test: load_bronze_tables loads CSV and Delta correctly 
# ---------------------------------------------------------
def test_load_bronze_tables(spark):
    paths = {
        "bronze": {
            "claim": "claim.csv",
            "contract": "contract.csv",
            "nse_id": "nse.delta"
        }
    }

    mock_reader = MagicMock()
    mock_reader.option.return_value = mock_reader
    mock_reader.csv.return_value = "CSV_DF"
    mock_reader.format.return_value.load.return_value = "NSE_DF"

    with patch.object(DataFrameReader, "__init__", return_value=None), \
         patch.object(DataFrameReader, "option", mock_reader.option), \
         patch.object(DataFrameReader, "csv", mock_reader.csv), \
         patch.object(DataFrameReader, "format", mock_reader.format):

        claim_df, contract_df, nse_df = load_bronze_tables(spark, paths)

    assert claim_df == "CSV_DF"
    assert contract_df == "CSV_DF"
    assert nse_df == "NSE_DF"



