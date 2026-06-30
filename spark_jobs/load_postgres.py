"""Load gold Delta tables into Postgres for the Grafana serving layer."""

import os
from spark_jobs.common.session import get_spark

LAKE_ROOT = os.environ["LAKE_ROOT"]
PG_URL = "jdbc:postgresql://postgres:5432/analytics"
PG_USER = "airflow"
PG_PASSWORD = "airflow"

spark = get_spark("load-postgres")

GOLD_TABLES = ["flights_per_hour", "traffic_by_origin_country", "current_snapshot"]

JDBC_OPTIONS = {
    "url": PG_URL,
    "user": PG_USER,
    "password": PG_PASSWORD,
    "driver": "org.postgresql.Driver",
    "truncate": "true"
}

for table in GOLD_TABLES:
    df = spark.read.format("delta").load(f"{LAKE_ROOT}/gold/{table}")
    (
        df.write
        .format("jdbc")
        .options(**JDBC_OPTIONS)
        .option("dbtable", table)
        .mode("overwrite")
        .save()
    )