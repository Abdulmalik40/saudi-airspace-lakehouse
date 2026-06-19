"""Explicit schemas for OpenSky data layers.

These schemas map OpenSky's positional state-vector arrays to named, typed
columns for downstream Spark processing.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    LongType,
    DoubleType,
    BooleanType,
    StringType,
    ArrayType,
    FloatType,
)

# State vector schema for OpenSky's /states/all endpoint.
# Each state vector arrives as a 17-element positional array.
# Reference: https://openskynetwork.github.io/opensky-api/rest.html
STATE_VECTOR_SCHEMA = StructType([
    StructField("icao24",StringType()),
    StructField("callsign",StringType()),
    StructField("origin_country",StringType()),
    StructField("time_position",IntegerType()),
    StructField("last_contact",IntegerType()),
    StructField("longitude",FloatType()),
    StructField("latitude",FloatType()),
    StructField("baro_altitude",FloatType()),
    StructField("on_ground",BooleanType()),
    StructField("velocity",FloatType()),
    StructField("true_track",FloatType()),
    StructField("vertical_rate",FloatType()),
    StructField("sensors",ArrayType(IntegerType()),nullable=True),
    StructField("geo_altitude",FloatType()),
    StructField("squawk",StringType()),
    StructField("spi",BooleanType()),
    StructField("position_source",IntegerType()),
    StructField("category",IntegerType()),
])
