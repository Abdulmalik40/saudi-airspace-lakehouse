import os 
from pyspark.sql.functions import col, count, countDistinct, date_trunc, row_number
from pyspark.sql.window import Window
from spark_jobs.common.session import get_spark


LAKE_ROOT = os.environ["LAKE_ROOT"]

spark = get_spark("silver-to-gold")

silver_path = f"{LAKE_ROOT}/silver/state_vectors"
gold_root = f"{LAKE_ROOT}/gold"

# 1. Read silver as a batch DataFrame (not streaming — gold runs as plain batch).
silver_df = spark.read.format("delta").load(silver_path)

# 2. Compute flights_per_hour: distinct aircraft per truncated hour of time_position.
flights_per_hour = (
    silver_df
    .withColumn("hour", date_trunc("hour", col("time_position")))
    .groupBy("hour")
    .agg(
        countDistinct("icao24").alias("distinct_aircraft"),
        count("*").alias("total_observations"),
    )
)

# 3. Compute traffic_by_origin_country: distinct aircraft and total observations per country.
traffic_by_country = silver_df.\
    groupBy("origin_country").agg(
        countDistinct("icao24").alias("distinct_aircraft"),
        count("*").alias("total_observations")
    )

# 4. Compute current_snapshot: latest row per icao24.
w = Window.partitionBy("icao24").orderBy(col("time_position").desc())
current_snapshot = (
    silver_df
    .withColumn("rn", row_number().over(w))
    .filter(col("rn") == 1)
    .drop("rn")
)

# 5. Write each as a Delta table with overwrite mode.
flights_per_hour.write.format("delta").mode("overwrite").save(f"{gold_root}/flights_per_hour")
traffic_by_country.write.format("delta").mode("overwrite").save(f"{gold_root}/traffic_by_origin_country")
current_snapshot.write.format("delta").mode("overwrite").save(f"{gold_root}/current_snapshot")