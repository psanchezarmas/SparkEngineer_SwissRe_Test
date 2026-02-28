# utils.py

import os
import yaml
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, LongType, DateType,
    DecimalType, IntegerType, ShortType, ByteType, FloatType, DoubleType,
    BooleanType, BinaryType
)

def load_yaml_schema(yaml_path: str) -> dict:
    """Load YAML file and return its content."""
    if not os.path.exists(yaml_path):
        raise FileNotFoundError(f"schemas.yaml not found: {yaml_path}")

    with open(yaml_path, "r") as f:
        return yaml.safe_load(f)


def parse_spark_type(type_str: str):
    """Convert YAML type string to Spark type."""
    type_map = {
        "string": StringType(),
        "timestamp": TimestampType(),
        "timestamp_ntz": TimestampType(),
        "date": DateType(),
        "long": LongType(),
        "bigint": LongType(),
        "int": IntegerType(),
        "integer": IntegerType(),
        "short": ShortType(),
        "smallint": ShortType(),
        "byte": ByteType(),
        "tinyint": ByteType(),
        "float": FloatType(),
        "double": DoubleType(),
        "boolean": BooleanType(),
        "binary": BinaryType(),
    }

    if type_str.startswith("decimal"):
        inside = type_str[type_str.find("(") + 1:type_str.find(")")]
        precision, scale = map(int, inside.split(","))
        return DecimalType(precision, scale)

    spark_type = type_map.get(type_str)
    if spark_type is None:
        raise ValueError(f"Unsupported type in YAML: {type_str}")

    return spark_type


def build_schema_from_yaml(config: dict, table_name: str) -> StructType:
    """Build Spark StructType from YAML config for a given table."""
    tables = config.get("tables", [])
    bronze_layer = next((t for t in tables if t.get("layer") == "bronze"), None)

    if not bronze_layer:
        raise ValueError("No bronze layer found in schemas.yaml")

    table = next(
        (tbl for tbl in bronze_layer.get("tables", [])
         if tbl.get("table_name") == table_name),
        None
    )

    if not table:
        raise ValueError(f"{table_name} not found in schemas.yaml")

    fields = [
        StructField(
            col["name"],
            parse_spark_type(col["type"]),
            col.get("nullable", True)
        )
        for col in table["columns"]
    ]

    return StructType(fields)
