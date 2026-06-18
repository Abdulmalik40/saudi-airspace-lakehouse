"""Throwaway test for fetch_states. Do not commit."""
from ingestion.opensky_client import fetch_states
import logging
logging.basicConfig(level=logging.INFO)
data = fetch_states()

print(f"Response keys: {list(data.keys())}")
print(f"Server time: {data.get('time')}")

states = data.get("states")
if states is None:
    print("No aircraft in bbox right now (states is None)")
else:
    print(f"Aircraft count: {len(states)}")
    print(f"First aircraft (raw state vector): {states[0]}")