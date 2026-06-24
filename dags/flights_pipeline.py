from airflow.sdk import dag, task
from ingestion.fetch_states import write_states_to_bronze
from datetime import datetime
import os
from pathlib import Path
import logging
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from airflow.sdk import get_current_context


logger = logging.getLogger(__name__)

HOST_PROJECT_DIR = os.environ["HOST_PROJECT_DIR"]

LAKE_ROOT = os.environ["LAKE_ROOT"]

@dag(
    schedule="*/5 * * * *",  
    start_date=datetime(2026, 6, 18),
    catchup=False,
    max_active_runs=1,
    tags=["flights", "opensky"],
)
def flights_pipeline():
    
    
    @task
    def ingest_states():
        """Ingest the current aircraft states and write to the bronze layer."""
        context = get_current_context()
        logical_date = context["logical_date"]
        output_path = write_states_to_bronze(logical_date, Path(LAKE_ROOT))
        logger.info("Ingested states to %s", output_path)
        
    
    bronze_to_silver = DockerOperator(
        task_id="bronze_to_silver",
        image="airspace-spark",
        container_name="airspace-spark-silver-{{ ts_nodash }}",
        api_version="auto",
        auto_remove="success",
        command="python -m spark_jobs.bronze_to_silver",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False,
        environment={
            "PYTHONPATH": "/opt/airflow",
            "LAKE_ROOT": "/opt/airflow/data",
        },
        mounts=[
            Mount(
                source=f"{HOST_PROJECT_DIR}/spark_jobs",
                target="/opt/airflow/spark_jobs",
                type="bind",
                read_only=True,
            ),
            Mount(
                source=f"{HOST_PROJECT_DIR}/data",
                target="/opt/airflow/data",
                type="bind",
                read_only=False,
            ),
            Mount(
                source=f"{HOST_PROJECT_DIR}/ingestion",
                target="/opt/airflow/ingestion",
                type="bind",
                read_only=True,
            ),
        ],
    )
    
    # Define dependencies
    ingest = ingest_states()
    ingest >> bronze_to_silver


flights_pipeline()