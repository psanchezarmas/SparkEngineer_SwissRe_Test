from unittest.mock import patch, MagicMock, mock_open
from spark_app.nse_pipeline import run_nse_pipeline, read_claim_ids
import os




def test_run_nse_pipeline_no_api_results(spark, tmp_path):
    with patch("spark_app.nse_pipeline.read_claim_ids", return_value=["CL1"]), \
         patch("spark_app.nse_pipeline.NSEProcessor") as MockProcessor:

        mock_proc = MockProcessor.return_value
        mock_proc.fetch_and_create_dataframe.return_value = spark.createDataFrame(
            [],
            "claim_id STRING, digest STRING"
        )

        run_nse_pipeline(spark, "fake.csv", str(tmp_path))

        mock_proc.write_bronze_table_nse.assert_not_called()

def test_run_nse_pipeline_delta_read_error(spark, tmp_path):
    with patch("spark_app.nse_pipeline.read_claim_ids", return_value=["CL1"]), \
         patch("spark_app.nse_pipeline.os.path.exists", return_value=True), \
         patch.object(spark.read, "format", side_effect=Exception("delta error")), \
         patch("spark_app.nse_pipeline.NSEProcessor") as MockProcessor:

        mock_proc = MockProcessor.return_value
        mock_proc.fetch_and_create_dataframe.return_value = spark.createDataFrame(
            [("CL1", "h1")], ["claim_id", "digest"]
        )

        run_nse_pipeline(spark, "fake.csv", str(tmp_path))

        mock_proc.write_bronze_table_nse.assert_called_once()

def test_main_loads_paths(monkeypatch):
    from spark_app import nse_pipeline

    fake_yaml = {"bronze": {"claim": "c.csv", "nse_id": "nse"}}

    monkeypatch.setattr(nse_pipeline, "SparkSession", MagicMock())
    monkeypatch.setattr(nse_pipeline, "run_nse_pipeline", MagicMock())

    with patch("builtins.open", mock_open(read_data="bronze:\n  claim: c.csv\n  nse_id: nse")):
        nse_pipeline.main()

    nse_pipeline.run_nse_pipeline.assert_called_once()

def test_run_nse_pipeline_no_claims(spark, tmp_path):
    with patch("spark_app.nse_pipeline.read_claim_ids", return_value=[]), \
         patch("spark_app.nse_pipeline.NSEProcessor") as MockProcessor:

        run_nse_pipeline(spark, "fake.csv", str(tmp_path))

        MockProcessor.assert_not_called()


def test_run_nse_pipeline_reads_existing_delta(spark, tmp_path):
    with patch("spark_app.nse_pipeline.read_claim_ids", return_value=["CL1", "CL2"]), \
         patch("spark_app.nse_pipeline.os.path.exists", return_value=True), \
         patch.object(spark.read, "format") as mock_format, \
         patch("spark_app.nse_pipeline.NSEProcessor") as MockProcessor:

        mock_load = MagicMock()
        mock_load.select.return_value.collect.return_value = [{"claim_id": "CL1"}]
        mock_format.return_value.load.return_value = mock_load

        mock_proc = MockProcessor.return_value
        mock_proc.fetch_and_create_dataframe.return_value = spark.createDataFrame(
            [("CL2", "h2")], ["claim_id", "digest"]
        )

        run_nse_pipeline(spark, "fake.csv", str(tmp_path))

        mock_proc.fetch_and_create_dataframe.assert_called_once()

        
def test_run_nse_pipeline_path(spark, tmp_path):
    fake_claims = ["CL1", "CL2"]

    with patch("spark_app.nse_pipeline.read_claim_ids", return_value=fake_claims), \
         patch("spark_app.nse_pipeline.MD4HashClient"), \
         patch("spark_app.nse_pipeline.NSEProcessor") as MockProcessor:

        mock_proc = MockProcessor.return_value
        mock_proc.fetch_and_create_dataframe.return_value = spark.createDataFrame(
            [("CL1", "h1"), ("CL2", "h2")],
            ["claim_id", "digest"]
        )

        output = str(tmp_path / "nse")

        run_nse_pipeline(spark, "fake.csv", output)

        mock_proc.fetch_and_create_dataframe.assert_called_once_with(fake_claims)
        mock_proc.write_bronze_table_nse.assert_called_once()
