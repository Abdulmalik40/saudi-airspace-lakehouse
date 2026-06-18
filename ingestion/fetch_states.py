from ingestion.opensky_client import fetch_states
from pathlib import Path
from datetime import datetime
import json
import logging

logger = logging.getLogger(__name__)
def write_states_to_bronze(logical_date: datetime, lake_root: Path) -> Path:
    """Write the raw states data to the bronze layer."""
    if logical_date.tzinfo is None:
        raise ValueError("logical_date must be timezone-aware")
    states = fetch_states()

    date_str = logical_date.strftime("%Y-%m-%d")
    hour_str = logical_date.strftime("%H")
    
    partition_dir = lake_root / "bronze" / f"ingest_date={date_str}" / f"ingest_hour={hour_str}"
    
    partition_dir.mkdir(parents=True, exist_ok=True)
    
    unix_ts = int(logical_date.timestamp())
    file_name = f"states_{unix_ts}.json"
    output_path = partition_dir / file_name
    
    with open(output_path, "w") as f:
        json.dump(states, f, indent=2)
        
    state_count = len(states.get("states") or [])
    file_size = output_path.stat().st_size
    logger.info("Wrote %d states to %s (%d bytes)",state_count, output_path, file_size)
    return output_path