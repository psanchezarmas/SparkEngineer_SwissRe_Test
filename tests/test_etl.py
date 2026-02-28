# """Tests for the ETL module."""
# from __future__ import annotations

# import os
# import tempfile
# from pyspark.sql import Row
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# from spark_app import etl


# def test_transform_data_simple(spark_session):
#     # create a small DataFrame with known data
#     schema = StructType(
#         [
#             StructField("category", StringType(), True),
#             StructField("value", IntegerType(), True),
#         ]
#     )
#     data = [Row(category="A", value=10), Row(category="A", value=20), Row(category="B", value=5)]
#     df = spark_session.createDataFrame(data, schema=schema)

#     result = etl.transform_data(df)
#     res = {row['category']: row['avg_value'] for row in result.collect()}
#     assert res['A'] == 15.0
#     assert res['B'] == 5.0


# def test_transform_data_missing_columns(spark_session):
#     df = spark_session.createDataFrame([Row(x=1)])
#     try:
#         etl.transform_data(df)
#         assert False, "Expected ValueError"
#     except ValueError as e:
#         assert "Missing required columns" in str(e)


# def test_run_end_to_end(tmp_path, spark_session):
#     # write an input CSV to tmp_path
#     input_file = tmp_path / "input.csv"
#     data = "category,value\nA,1\nA,3\nB,2\n"
#     input_file.write_text(data)

#     output_dir = tmp_path / "out"
#     # call run; it should create the output directory with parquet files
#     etl.run(spark_session, str(input_file), str(output_dir))

#     # read the output back and verify contents
#     out_df = spark_session.read.parquet(str(output_dir))
#     collected = {row['category']: row['avg_value'] for row in out_df.collect()}
#     assert collected == {'A': 2.0, 'B': 2.0}
