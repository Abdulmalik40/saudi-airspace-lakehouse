"""Throwaway test for write_states_to_bronze. Do not commit."""
from datetime import datetime, timezone
from pathlib import Path

from ingestion.fetch_states import write_states_to_bronze

# Use current time as the logical_date, simulating what Airflow would pass
logical_date = datetime.now(timezone.utc)
lake_root = Path("/opt/airflow/data")

output_path = write_states_to_bronze(logical_date, lake_root)

print(f"Wrote file to: {output_path}")
print(f"File exists: {output_path.exists()}")
print(f"File size: {output_path.stat().st_size} bytes")