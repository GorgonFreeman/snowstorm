# snowstorm

GA4 to Snowflake data connector (proof of concept).

## Ingestion model

**Goal:** Stay under Google’s standard-property **daily batch** export cap by treating **`events_intraday_*` (streaming)** as the primary feed, then **folding in** **`events_*` (daily)** when it is the **stable snapshot** for that calendar day.

### 1. Incremental intraday (streaming)

- **Source:** `analytics_<property_id>.events_intraday_YYYYMMDD` in BigQuery.
- **Cadence:** Run on an interval (e.g. 15–60 minutes).
- **Cursor:** Persist a **high-water mark** per property and calendar date (e.g. last seen `event_timestamp`, or a composite if needed for ties).
- **Load:** Append **new rows** into a single Snowflake fact table (same table the daily merge targets).

### 2. Reconciliation: daily snapshot

- **Source:** `analytics_<property_id>.events_YYYYMMDD` once that day’s batch table is the **authoritative** completion (per Google’s export behavior and your ops checks).
- **Action:** **Merge** into the **same** Snowflake fact table so **daily rows supersede** any overlapping intraday rows for that date.
- **Dedupe key:** Use a stable row identity acceptable for GA4 exports — at minimum combine fields that uniquely identify an exported event in practice (e.g. `user_pseudo_id`, `event_timestamp`, `event_name`, and positional/index fields if present in the export). **Tighten this key** once you inspect real shards in BigQuery for your property.

### 3. Why this order

- **Intraday** carries **unbounded event volume** for standard properties (streaming has **no** 1M/day event cap in Google’s BigQuery export limits).
- **Daily** gives a **stable** table for reporting when it is complete; merged **after** intraday prevents double-counting and aligns Snowflake with Google’s finalized day where the batch export is healthy.

### 4. Operational checks

- Compare row counts or key metrics between BigQuery **`events_*`** and **`events_intraday_*`** for the same `YYYYMMDD` before trusting merge timing.
- If **daily export is paused or incomplete** for your volume, **do not** treat **`events_*`** as complete — keep loading **intraday only** for that date until the situation is resolved (filtering, 360, etc.).
