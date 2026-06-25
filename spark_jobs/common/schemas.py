"""Explicit schemas for OpenSky data layers.

These schemas map OpenSky's positional state-vector arrays to named, typed
columns for downstream Spark processing.
"""

from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType


# Wrapper schema for one OpenSky /states/all response file.
# Each file contains:
#   - time:   snapshot epoch seconds
#   - states: array of positional state-vector arrays (17 or 18 elements each,
#             read as array<string>; downstream code projects and casts by index)
BRONZE_WRAPPER_SCHEMA = StructType([
    StructField("time", LongType(), True),
    StructField("states", ArrayType(ArrayType(StringType())), True),
])