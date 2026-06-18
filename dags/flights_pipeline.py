from airflow.decorators import dag, task
from ingestion.fetch_states import write_states_to_bronze
from datetime import datetime, timezone
import os
from pathlib import Path
import logging
"""DAG for periodic ingestion of OpenSky aircraft states into bronze."""

logger = logging.getLogger(__name__)

@dag(
    schedule="*/5 * * * *",  # Every 5 minutes
    start_date = datetime(2026,6,18),
    catchup=False,
    max_active_runs=1,
    tags=["flights", "opensky"],
)
def flights_pipeline():
    @task
    def ingest_states(logical_date: datetime):
        """Ingest the current aircraft states and write to the bronze layer."""
        lake_root = Path(os.environ["LAKE_ROOT"])
        output_path = write_states_to_bronze(logical_date, lake_root)
        logger.info("Ingested states to %s", output_path)
    ingest_states()
    
flights_pipeline()
