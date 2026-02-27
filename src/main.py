# Initial code for running docker

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DockerSparkTest") \
    .master("local[*]") \
    .getOrCreate()

df = spark.createDataFrame(
    [("Pablo", 25), ("Lucía", 30)],
    ["name", "age"]
)

df.show()

spark.stop()
