# Ingestion Layer

This folder contains the code that pulls aircraft data from the OpenSky Network and writes it to bronze. Three files, each with one clear job.

```
ingestion/
├── token_manager.py    # Handles OAuth2 authentication
├── opensky_client.py   # Calls the OpenSky API
└── fetch_states.py     # Writes the result to bronze
```

The layering is deliberate: each file knows about the one below it, never about the one above. `fetch_states.py` calls `opensky_client.py`, which uses `token_manager.py`. None of them know that Airflow exists. That separation is what makes them testable, replaceable, and easy to reason about.

---

## `token_manager.py` — Authentication

### What it does

OpenSky requires every API call to include an access token in the request header. That token isn't permanent — it expires after about 30 minutes. This file is responsible for getting tokens, keeping them, and refreshing them when they expire.

### How it works

The `TokenManager` class holds two pieces of state in memory:

- The current access token (a long string)
- When that token expires (a UTC timestamp)

When you call `get_token()`, the manager checks the cached token. If it's still valid, return it directly. If it's expired or missing, call OpenSky's authentication endpoint to fetch a fresh one, store it, and return it.

This caching matters. OpenSky's tokens last 30 minutes. The pipeline polls every 5 minutes. Without caching, every poll would do two API calls: one to get a token and one to fetch data. With caching, every token serves six polls. Six times less load on the authentication endpoint, six times less risk of running into auth-side rate limits.

### Why we built it as a class

The token and its expiration are linked — they only make sense together. A class groups them naturally with the methods that read and update them. A separate dict or global variables would work but be harder to reason about.

The class is instantiated once at module level inside `opensky_client.py`. There's one TokenManager for the whole process, shared by every call to `fetch_states()`. That's what makes the caching work — if every call created its own TokenManager, every cache would be empty.

### Failure handling

Three things can go wrong when fetching a token:

1. **The network drops.** `tenacity` retries the call up to 3 times with exponential backoff (2 seconds, 4 seconds, then up to 10).
2. **OpenSky's server returns a 5xx error.** Same retry behavior — these are transient.
3. **Credentials are wrong.** A 4xx error. No retry — retrying a permanent failure just wastes time. The exception bubbles up so the pipeline fails loudly.

The pattern is: retry transient errors, fail fast on permanent ones. That's the difference between a system that recovers and one that hides bugs.

### The 30-second refresh margin

Tokens are refreshed 30 seconds before they actually expire. The reason: clock skew. The token might say "expires at 13:30:00 UTC," but your machine's clock and OpenSky's might disagree by a second or two. Refreshing 30 seconds early eliminates that race condition. The cost is one extra token fetch every 30 minutes. The benefit is no 401 errors from "the token was valid 0.4 seconds ago, why is it rejected?"

---

## `opensky_client.py` — The API call

### What it does

One function: `fetch_states()`. It calls OpenSky's `/states/all` endpoint with the MENA bounding box and returns the raw JSON response as a Python dict.

### What it doesn't do

This is important. The client doesn't:

- Write anything to disk
- Parse the state vectors into typed rows
- Validate the data
- Decide where it goes

It returns raw OpenSky data, unchanged. The next layer up (`fetch_states.py`) decides what to do with it.

This separation matters because it means the client is reusable. If you ever wanted to fetch OpenSky data into a different system — a Jupyter notebook, a one-off analysis, a different lakehouse — you call `fetch_states()` and get the same dict. No assumptions baked in.

### The bounding box

The MENA region is hardcoded in `.env` as four floats:

```
BBOX_LAMIN=12.0   # southern edge (Yemen)
BBOX_LAMAX=42.0   # northern edge (Turkey)
BBOX_LOMIN=25.0   # western edge (Egypt)
BBOX_LOMAX=65.0   # eastern edge (Iran)
```

These are read at module import time and validated. If any are missing or unparseable, the module refuses to load. That's intentional — better to fail at startup than 5 minutes into production when the first API call fires.

### Rate limit observability

After every successful call, the client reads two response headers and logs them:

- `X-Rate-Limit-Remaining` — how many credits OpenSky still allows you today
- `X-Rate-Limit-Retry-After-Seconds` — only present on 429 responses

Logging the remaining credits is a small but valuable habit. When the pipeline has been running for a week and you check the logs, you can see the credit usage pattern at a glance. If something goes wrong and your DAG starts polling 100 times per minute by accident, the credit count plummets and you spot it before you hit zero.

### Retry policy — slightly different from TokenManager

The client retries on the same network errors as TokenManager, plus two extras:

- 5xx HTTP errors (OpenSky is having a bad moment)
- 429 (rate limited — backing off and retrying is exactly what you should do)

These cases are handled by a custom predicate `_is_retryable()`. Tenacity calls it on every exception to decide whether to retry. The predicate is small and explicit — much clearer than chaining decorators for each exception type.

---

## `fetch_states.py` — Writing to bronze

### What it does

Takes the data from `fetch_states()` and writes it to disk in a partitioned folder structure. One function: `write_states_to_bronze()`.

### The partitioning scheme

The function writes files into a directory tree like this:

```
data/bronze/
└── ingest_date=2026-06-18/
    └── ingest_hour=10/
        ├── states_1781777409.json
        ├── states_1781777709.json
        └── states_1781778009.json
```

The `key=value` directory naming is **Hive-style partitioning**. Every data engine on Earth recognizes it. Spark, Trino, DuckDB, Athena — all of them see `ingest_date=2026-06-18` and understand that this directory contains data for that date, without having to open any of the files.

This matters for performance. When Spark reads bronze in Week 3, you can tell it "only look at the last hour" and Spark will skip every other partition without opening a single file. At 5-minute polling for a month, that's 8,000+ files. Reading 12 of them vs 8,000 is a 600x speedup.

### Why partition by date AND hour, not just date

By ship day you'll have around 8,000 files in bronze. If they were all in one folder, every operation (listing, stat-ing, even just opening the file manager) would slow down. Splitting by hour keeps each folder to about 12 files — always small, always fast.

You could partition by minute. Don't. That would create 1,400 folders per day with one file each — pure overhead for no benefit. The rule for partition granularity: coarse enough that each partition has many files, fine enough that queries can skip large chunks.

### Why filenames include a Unix timestamp

The filename `states_1781777409.json` encodes the moment the data was collected. Three reasons this is the right pattern:

1. **Sortable** — alphabetic sort equals chronological sort. `ls` shows files in order automatically.
2. **Unique** — two runs in the same minute don't collide.
3. **Reversible** — you can convert the filename back to a wall-clock time when debugging.

### Logical date vs current time

The function takes `logical_date` as a parameter rather than calling `datetime.now()` internally. The reason matters.

Airflow tasks have a *scheduled time* (when the task was meant to run) and an *actual time* (when the worker picked it up). These usually agree, but not always — workers can be busy, tasks can be backfilled, retries happen.

When data lands in bronze, it should be partitioned by what it *represents*, not when it happened to be written. If you backfill a missing 09:05 task at midnight, the file should go in `ingest_hour=09`, not `ingest_hour=00`. Using `logical_date` from Airflow makes this automatic.

The function also asserts that `logical_date` is timezone-aware. Naive datetimes silently use system local time, which gives different Unix timestamps depending on where you run the code. Refusing naive inputs prevents a class of bugs that only appear in production.

### Bronze is "evidence, not data"

The file written here is the raw OpenSky response, unchanged. No cleaning, no validation, no type conversion. That's deliberate.

If something looks wrong three weeks from now — a strange aircraft, an impossible altitude, a missing field — you can go back to the original bronze file and see exactly what OpenSky sent you. If you had cleaned the data on the way in, you'd have lost the evidence. Bronze is the audit log; everything downstream can be reprocessed from it.

The trade-off is storage. Bronze files are slightly larger than they need to be. For this project's data volume, that doesn't matter. For a production system at scale, you'd compress them — but you still wouldn't transform them.

### What this function doesn't do

- No locking or concurrency control. If two tasks ever wrote to the same path at the same time, one would overwrite the other. We avoid this by giving each task a unique logical_date and using it in the filename.
- No atomic writes. If the process crashed mid-write, you'd get a partial JSON file. For local development this is acceptable; in W4 on cloud storage we'd revisit.
- No retries. The function trusts that `fetch_states()` already handled transient API failures. If writing to disk fails, that's a real problem worth bubbling up.

---

## How they connect

When `write_states_to_bronze()` is called:

1. It calls `fetch_states()` from `opensky_client.py`
2. Which calls `_token_manager.headers()` from the module-level TokenManager
3. Which returns a cached token (or fetches a fresh one if expired)
4. The client sends an authenticated GET request to OpenSky
5. The response is parsed as JSON and returned up the stack
6. `write_states_to_bronze()` builds a partition path from `logical_date`
7. Creates the directory if needed
8. Writes the JSON to a uniquely-named file
9. Logs what it wrote and returns the path

Each layer does one thing. If something breaks, you know where to look.

---

## What's still ahead

These files handle the "raw data into bronze" step. They don't yet run on a schedule — that comes next, with a real Airflow DAG (`flights_pipeline.py`) that calls `write_states_to_bronze()` every five minutes.

After that, Week 3 work: Spark Structured Streaming reads bronze, transforms into silver, then gold marts, and serves the result to a Grafana dashboard. The ingestion code you have now stays unchanged — that's the point of the layered design.