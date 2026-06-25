# saudi-airspace

A data pipeline that collects live aircraft positions over Saudi Arabia and the
wider MENA region, stores them in a Delta Lake.

## Problem

The OpenSky Network publishes the position, altitude, and velocity of every
aircraft tracked by its receivers. Over the MENA region this is hundreds of
aircraft at any momen.
## Scope

In scope:

- Scheduled ingestion of MENA airspace state vectors every five minutes
- A bronze, silver, and gold layout in Delta Lake
- Aggregate tables: flights per hour, traffic by origin country, current snapshot

Out of scope:

- Real-time streaming. The five-minute cadence is enough for the use case and
  avoids the cost and complexity of a true streaming system.
- Machine learning or prediction.
- Worldwide coverage. The OpenSky free quota and the regional focus make this
  a deliberate choice.











