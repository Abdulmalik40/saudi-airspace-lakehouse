import os
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType
from pyspark.sql.functions import col, current_timestamp, explode, expr
from delta.tables import DeltaTable
from spark_jobs.common.session import get_spark
from spark_jobs.common.schemas import BRONZE_WRAPPER_SCHEMA
LAKE_ROOT = os.environ["LAKE_ROOT"]

spark = get_spark("bronze-to-silver")

bronze_path = f"{LAKE_ROOT}/bronze"
silver_path = f"{LAKE_ROOT}/silver/state_vectors"
checkpoint_path = f"{LAKE_ROOT}/checkpoints/bronze_to_silver"



bronze_df = (
    spark.readStream
    .schema(BRONZE_WRAPPER_SCHEMA)
    .option("multiLine", "true")
    .option("recursiveFileLookup", "true")
    .json(bronze_path)
)


def process_batch(batch_df: DataFrame, batch_id: int):
    """Explode positional state arrays, project by index, cast types, MERGE"""

   
    exploded = batch_df.select(explode(col("states")).alias("s"))

    # Project by positional index and cast to real types.
    # OpenSky state vector positions:
    # 0=icao24, 1=callsign, 2=origin_country, 3=time_position, 4=last_contact,
    # 5=longitude, 6=latitude, 7=baro_altitude, 8=on_ground, 9=velocity,
    # 10=true_track, 11=vertical_rate, 12=sensors (skipped), 13=geo_altitude,
    # 14=squawk, 15=spi, 16=position_source, 17=category (may be absent)
    projected = exploded.select(
        col("s")[0].alias("icao24"),
        col("s")[1].alias("callsign"),
        col("s")[2].alias("origin_country"),
        col("s")[3].cast("long").alias("time_position"),
        col("s")[4].cast("long").alias("last_contact"),
        col("s")[5].cast("double").alias("longitude"),
        col("s")[6].cast("double").alias("latitude"),
        col("s")[7].cast("double").alias("baro_altitude"),
        col("s")[8].cast("boolean").alias("on_ground"),
        col("s")[9].cast("double").alias("velocity"),
        col("s")[10].cast("double").alias("true_track"),
        col("s")[11].cast("double").alias("vertical_rate"),
        col("s")[13].cast("double").alias("geo_altitude"),
        col("s")[14].alias("squawk"),
        col("s")[15].cast("boolean").alias("spi"),
        col("s")[16].cast("int").alias("position_source"),
        expr("try_cast(get(s, 17) as int)").alias("category"),
    )

    final_df = (
        projected
        .filter(col("time_position").isNotNull())
        .withColumn("time_position", col("time_position").cast("timestamp"))
        .withColumn("last_contact", col("last_contact").cast("timestamp"))
        .withColumn("silver_ingested_at", current_timestamp())
    )

    if DeltaTable.isDeltaTable(spark, silver_path):
        delta_table = DeltaTable.forPath(spark, silver_path)
        (
            delta_table.alias("target")
            .merge(
                final_df.alias("source"),
                "target.icao24 = source.icao24 "
                "AND target.time_position = source.time_position"
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        final_df.write.format("delta").save(silver_path)


query = (
    bronze_df.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()