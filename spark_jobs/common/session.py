"""SparkSession factory for project lakehouse jobs."""

import os
from pyspark.sql import SparkSession


JAR_DIR = "/opt/spark-extra-jars"
JAR_PATHS = ",".join([
    f"{JAR_DIR}/delta-spark.jar",
    f"{JAR_DIR}/delta-storage.jar",
    f"{JAR_DIR}/postgresql.jar",
])


def get_spark(app_name: str = "spark-job") -> SparkSession:
    """Build a SparkSession configured for the project lakehouse.

    Includes Delta Lake extensions, the bundled JARs, and Azure ADLS
    authentication when LAKE_ROOT is an abfss:// URI.
    """
    lake_root = os.environ["LAKE_ROOT"]

    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars", JAR_PATHS)
    )

    # TODO: untested — wire up in W4 when Azure resources exist
    if lake_root.startswith("abfss://"):
        builder = _add_azure_config(builder)

    return builder.getOrCreate()


def _add_azure_config(builder: SparkSession.Builder) -> SparkSession.Builder:
    """Add Azure ADLS Gen2 service principal auth to the SparkSession builder."""
    account = os.environ["AZURE_STORAGE_ACCOUNT"]
    tenant = os.environ["AZURE_TENANT_ID"]
    client_id = os.environ["AZURE_CLIENT_ID"]
    secret = os.environ["AZURE_CLIENT_SECRET"]

    return (
        builder
        .config(f"spark.hadoop.fs.azure.account.auth.type.{account}.dfs.core.windows.net", "OAuth")
        .config(f"spark.hadoop.fs.azure.account.oauth.provider.type.{account}.dfs.core.windows.net",
                "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
        .config(f"spark.hadoop.fs.azure.account.oauth2.client.id.{account}.dfs.core.windows.net", client_id)
        .config(f"spark.hadoop.fs.azure.account.oauth2.client.secret.{account}.dfs.core.windows.net", secret)
        .config(f"spark.hadoop.fs.azure.account.oauth2.client.endpoint.{account}.dfs.core.windows.net",
                f"https://login.microsoftonline.com/{tenant}/oauth2/token")
    )