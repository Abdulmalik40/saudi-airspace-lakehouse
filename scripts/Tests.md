# Scripts

Runnable examples for exploring the ingestion layer outside of Airflow. These are not part of the production pipeline — they're for testing, debugging, and demonstration.

## Prerequisites

The stack must be running:

\`\`\`
make up
\`\`\`

Run each script inside the airflow-scheduler container:

\`\`\`
docker compose exec airflow-scheduler python /opt/airflow/scripts/<name>.py
\`\`\`

## What each script does

### `try_token.py`

Fetches an OAuth2 token from OpenSky and prints metadata about it (length, expiration, cache behavior). Useful for verifying credentials are set correctly.

### `try_fetch.py`

Calls the OpenSky API and prints a summary of the response — server time, aircraft count, and one example state vector. Verifies the client can authenticate and retrieve real data.

### `try_write.py`

End-to-end: fetches data and writes it to the bronze layer at \`data/bronze/\`. Verifies the full ingestion path works. Each run creates a new JSON file in a partitioned folder structure.

## Note on credits

Each run of \`try_fetch.py\` and \`try_write.py\` consumes 4 OpenSky API credits. OpenSky's free tier allows 4,000 credits per day, so casual experimentation is fine.