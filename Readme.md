# saudi-airspace-lakehouse

A data pipeline that collects live aircraft positions over Saudi Arabia and the
wider MENA region, stores them in a Delta Lake, and serves aggregates to a
Grafana dashboard.

## Problem

The OpenSky Network publishes the position, altitude, and velocity of every
aircraft tracked by its receivers. Over the MENA region this is hundreds of
aircraft at any moment, including transit flights between Europe and Asia.
The data is public, but it is only available as a live snapshot. There is no
analytical history of who flew through this airspace last week, last month,
or yesterday.

This project keeps that history. It polls OpenSky every five minutes, stores
the raw responses, builds clean tables on top of them, and exposes aggregates
through a dashboard.

## Scope

In scope:

- Scheduled ingestion of MENA airspace state vectors every five minutes
- A bronze, silver, and gold layout in Delta Lake
- Aggregate tables: flights per hour, traffic by origin country, current snapshot
- A Grafana dashboard backed by Postgres
- Quality checks that stop the pipeline when something looks wrong

Out of scope:

- Real-time streaming. The five-minute cadence is enough for the use case and
  avoids the cost and complexity of a true streaming system.
- Machine learning or prediction.
- Worldwide coverage. The OpenSky free quota and the regional focus make this
  a deliberate choice.

## Stack

Airflow, Spark (Structured Streaming and batch), Delta Lake, Postgres, Grafana,
Docker Compose. The cloud version uses Azure Data Lake Storage Gen2 and
Databricks for the silver-to-gold step.









