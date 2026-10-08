# SG Carpark Availability Predictor — Data Model & Data Quality

## Layer Overview

The lake on S3 uses medallion zones. Bronze stays raw and append-only. Silver and gold are Iceberg tables managed by dbt.

```
┌────────────────┐    ┌─────────────────────┐    ┌──────────────────────────────┐    ┌──────────────────┐
│ BRONZE (raw)   │    │ SILVER (cleaned)    │    │ GOLD (modelled)              │    │ SERVING          │
│ Parquet, Hive  │──► │ Iceberg             │──► │ Iceberg                      │──► │ Parquet extracts │
│ partitions UTC │    │ typed, deduped, SGT │    │ star schema + marts          │    │ for Streamlit    │
│                │    │                     │    │                              │    │                  │
│ carpark_       │    │ stg_carpark_        │    │ dim_carpark  dim_date        │    │ baseline.parquet │
│ availability_  │    │ availability        │    │ dim_time_bucket              │    │ carparks.parquet │
│ raw            │    │ snap_carpark (SCD2) │    │ fct_carpark_availability     │    │ holidays.parquet │
│                │    │                     │    │ mart_* (baseline, hourly…)   │    │ manifest.json    │
└────────────────┘    └─────────────────────┘    └──────────────────────────────┘    └──────────────────┘
   written by            written by dbt             written by dbt                     written by
   Lambda                (tag: silver)              (tag: gold)                        export_serving task
```

**Estimated volume:** about 2,000–3,000 records per poll (one per carpark × lot type; check this on day 1) × 288 polls/day ≈ **0.6–0.9M rows/day**, or ~35–50M rows over 8 weeks. Small for Athena.

---

## Source: `CarParkAvailabilityv2` Response

`GET https://datamall2.mytransport.sg/ltaodataservice/CarParkAvailabilityv2?$skip=<n>` with header `AccountKey: <key>`. Results are in the `value` array, at most 500 per page. Confirm the base URL against the current LTA DataMall API User Guide.

| API Field | Example | Bronze Column | Notes |
|---|---|---|---|
| `CarParkID` | `"HE12"` | `carpark_id` | **Not guaranteed unique across agencies**, so always key on `carpark_id + agency` |
| `Area` | `"Marina"` | `area` | Often empty for some agencies |
| `Development` | `"Suntec City"` | `development` | Display name |
| `Location` | `"1.29375 103.85718"` | `location` | One string: `"<lat> <lon>"`, split in silver; can be empty |
| `AvailableLots` | `213` | `available_lots` | Integer ≥ 0 |
| `LotType` | `"C"` | `lot_type` | `C` = car, `H` = heavy vehicle, `Y` = motorcycle |
| `Agency` | `"HDB"` | `agency` | `HDB`, `LTA`, `URA` |
| *(added by Lambda)* | | `polled_at_utc`, `poll_id` | The API has no per-record timestamp, so poll time = snapshot time |

---

## S3 Layout & Glue Databases

```
s3://carpark-lake-<account_id>/
├── bronze/carpark_availability/dt=YYYY-MM-DD/hour=HH/poll_<YYYYMMDDTHHMMSSZ>.parquet   (UTC)
├── quarantine/carpark_availability/dt=YYYY-MM-DD/poll_<ts>.json
├── silver/<table>/<uuid>/…        ← dbt-athena, s3_data_naming = schema_table_unique
├── gold/<table>/<uuid>/…
├── dev/…                          ← dbt dev target (CI + local development)
└── serving/
    ├── baseline.parquet
    ├── carparks.parquet
    ├── holidays.parquet
    └── manifest.json

s3://carpark-athena-results-<account_id>/   ← Athena query results (lifecycle: delete after 7 days)
```

| Glue Database | Contents | Created By |
|---|---|---|
| `bronze` | `carpark_availability_raw` (external table, partition projection) | Terraform |
| `silver` | `stg_carpark_availability`, `snap_carpark`, seed tables | Terraform (database), dbt (tables) |
| `gold` | `dim_*`, `fct_*`, `mart_*` | Terraform (database), dbt (tables) |

---

## Bronze: `bronze.carpark_availability_raw`

Raw, append-only, never modified. This is the **source of truth for every replay**.

```sql
CREATE EXTERNAL TABLE bronze.carpark_availability_raw (
  carpark_id      string,
  area            string,
  development     string,
  location        string,
  available_lots  bigint,
  lot_type        string,
  agency          string,
  polled_at_utc   timestamp,
  poll_id         string
)
PARTITIONED BY (dt string, hour string)
STORED AS PARQUET
LOCATION 's3://carpark-lake-<account_id>/bronze/carpark_availability/'
TBLPROPERTIES (
  'projection.enabled'          = 'true',
  'projection.dt.type'          = 'date',
  'projection.dt.format'        = 'yyyy-MM-dd',
  'projection.dt.range'         = '2026-10-01,NOW',
  'projection.dt.interval'      = '1',
  'projection.dt.interval.unit' = 'DAYS',
  'projection.hour.type'        = 'integer',
  'projection.hour.range'       = '0,23',
  'projection.hour.digits'      = '2',
  'storage.location.template'   = 's3://carpark-lake-<account_id>/bronze/carpark_availability/dt=${dt}/hour=${hour}/'
);
```

> **⚠️ Provision this table in Terraform** (`aws_glue_catalog_table` with the same `parameters`), not by hand. In HCL, escape the template variables as `$${dt}` and `$${hour}`.

> **⚠️ Column types must match what Lambda writes.** Write `available_lots` as int64 and `polled_at_utc` as a timezone-naive UTC timestamp. A type mismatch shows up as Athena `HIVE_BAD_DATA` errors.

### Quarantine record shape

```json
{ "poll_id": "20261012T020512Z", "polled_at_utc": "2026-10-12T02:05:12Z",
  "reason": "available_lots not an integer", "record": { "...original API record..." } }
```

---

## Silver

### `silver.stg_carpark_availability`

| Property | Value |
|---|---|
| Materialization | `incremental`, `table_type='iceberg'`, `incremental_strategy='merge'` |
| Unique key | `carpark_id, agency, lot_type, snapshot_slot_sgt` |
| Partitioning | `day(snapshot_slot_sgt)` (Iceberg hidden partition) |
| Incremental window | Bronze rows where `dt`/`hour` and `polled_at_utc` fall in `[window_start_utc, window_end_utc)` (dbt vars from the DAG) |
| Tag | `silver` |

**Transformation rules:**

| Output Column | Rule |
|---|---|
| `carpark_id` | `trim(carpark_id)` |
| `agency` | `upper(trim(agency))` |
| `lot_type` | `upper(trim(lot_type))` |
| `available_lots` | `cast(available_lots as integer)` |
| `development`, `area` | `nullif(trim(x), '')` |
| `latitude`, `longitude` | `try_cast(split_part(location, ' ', 1/2) as double)`; NULL if unparseable |
| `polled_at_utc` | `cast(polled_at_utc as timestamp(6))` |
| `polled_at_sgt` | `cast(polled_at_utc AT TIME ZONE 'Asia/Singapore' as timestamp(6))` |
| `snapshot_slot_sgt` | `date_add('minute', -(minute(polled_at_sgt) % 5), date_trunc('minute', polled_at_sgt))` |
| Deduplication | `row_number() over (partition by carpark_id, agency, lot_type, snapshot_slot_sgt order by polled_at_utc desc) = 1` (retries can land two polls in one slot) |

> **⚠️ Iceberg on Athena only accepts `timestamp(6)`.** Cast every timestamp column explicitly or the merge fails with a precision error.

> **⚠️ Timezone rule:** Bronze is UTC. This model is the **only** place UTC becomes SGT. Every downstream column (date, bucket, day type) is SGT.

### `silver.snap_carpark` (dbt snapshot, SCD Type 2)

| Property | Value |
|---|---|
| Source | Latest observation per carpark from `stg_carpark_availability` (one row per `carpark_id + agency`; lot types collapsed) |
| Unique key | `concat(carpark_id, '-', agency)` |
| Strategy | `check` on `development`, `area`, `latitude`, `longitude` |
| Table type | Iceberg |
| Generated columns | `dbt_valid_from`, `dbt_valid_to`, `dbt_scd_id`, `dbt_updated_at` |

---

## Gold: Star Schema

```
                        ┌───────────────────────┐
                        │ dim_carpark (SCD2)    │
                        │ carpark_key (PK)      │
                        │ carpark_id, agency    │
                        │ development, area     │
                        │ latitude, longitude   │
                        │ valid_from, valid_to  │
                        │ is_current            │
                        └──────────┬────────────┘
                                   │
┌───────────────────┐   ┌──────────┴───────────────┐   ┌──────────────────────┐
│ dim_date          │   │ fct_carpark_availability │   │ dim_time_bucket      │
│ date_key (PK)     │───│ availability_key (PK)    │───│ time_bucket_key (PK) │
│ date, day_of_week │   │ carpark_key (FK)         │   │ bucket_start (HH:MM) │
│ is_weekend        │   │ date_key (FK)            │   │ bucket_end           │
│ is_public_holiday │   │ time_bucket_key (FK)     │   │ hour, half (0/1)     │
│ holiday_name      │   │ carpark_id, agency       │   │ period (am/pm/…)     │
│ day_type          │   │ lot_type                 │   └──────────────────────┘
└───────────────────┘   │ snapshot_slot_sgt        │
                        │ available_lots  (fact)   │
                        └──────────────────────────┘
```

### Grain Definition

| Business Event | Facts | Atomic Grain | Dimensions |
|---|---|---|---|
| A carpark reports its available lots | `available_lots` | One carpark × lot type × 5-minute snapshot slot (SGT) | Carpark, date, time bucket (lot type kept as a degenerate dimension) |

### `gold.dim_carpark`

| Column | Type | Rule |
|---|---|---|
| `carpark_key` | string | `generate_surrogate_key(['carpark_id','agency','dbt_valid_from'])` |
| `carpark_id`, `agency` | string | Natural key |
| `development`, `area` | string | From snapshot |
| `latitude`, `longitude` | double | From snapshot |
| `valid_from` | timestamp(6) | `dbt_valid_from`, **except the first version of each carpark, which becomes `1900-01-01`** |
| `valid_to` | timestamp(6) | `coalesce(dbt_valid_to, timestamp '9999-12-31')` |
| `is_current` | boolean | `dbt_valid_to is null` |

> **⚠️ Why backdate the first version?** The snapshot runs *after* the hour it describes, so `dbt_valid_from` is later than that hour's facts. Without backdating, the first hour of facts for every carpark fails the dimension lookup.

### `gold.dim_date`

Generated with `dbt_utils.date_spine` (2026-10-01 → 2027-12-31) and left-joined to the `sg_public_holidays` seed.

| Column | Type | Rule |
|---|---|---|
| `date_key` | integer | `yyyymmdd` |
| `date` | date | |
| `day_of_week` | integer | 1 = Monday … 7 = Sunday |
| `is_weekend` | boolean | `day_of_week in (6, 7)` |
| `is_public_holiday` | boolean | Date exists in seed |
| `holiday_name` | string | From seed |
| `day_type` | string | `'weekend_holiday'` if weekend or public holiday, else `'weekday'` |

Seed `sg_public_holidays.csv`: `date,holiday_name`, copied from MOM's official list. Add a reminder to update it every December.

### `gold.dim_time_bucket`

48 rows generated from `sequence(0, 47)`.

| Column | Example | Rule |
|---|---|---|
| `time_bucket_key` | `20` | `hour * 2 + (minute >= 30)` |
| `bucket_start` / `bucket_end` | `10:00` / `10:29` | |
| `hour`, `half` | `10`, `0` | |
| `period` | `morning` | `night` 0–5, `morning` 6–11, `afternoon` 12–17, `evening` 18–23 |

### `gold.fct_carpark_availability`

| Property | Value |
|---|---|
| Materialization | `incremental`, Iceberg, `merge` on `availability_key` |
| Partitioning | `day(snapshot_slot_sgt)` |
| Source | `stg_carpark_availability` (same window as the stg run) |
| Dimension lookups | `dim_carpark` on `carpark_id + agency` where `snapshot_slot_sgt` is in `[valid_from, valid_to)`; `dim_date` on date; `dim_time_bucket` on bucket |

| Column | Rule |
|---|---|
| `availability_key` | `generate_surrogate_key(['carpark_id','agency','lot_type','snapshot_slot_sgt'])` |
| `carpark_key`, `date_key`, `time_bucket_key` | FK lookups above |
| `carpark_id`, `agency`, `lot_type` | Carried for convenience (also used to filter) |
| `snapshot_slot_sgt` | From stg |
| `available_lots` | The fact |

---

## Gold: Marts

### `gold.mart_availability_baseline` (the prediction table)

| Property | Value |
|---|---|
| Materialization | `table` (Iceberg), fully rebuilt each run (≈ a materialized view) |
| Grain | carpark × agency × lot type × day type × time bucket |
| Lookback | `var('baseline_lookback_days', 56)`: a rolling 8 weeks, so it adapts to changes |

```sql
select
  f.carpark_id, f.agency, f.lot_type, d.day_type, f.time_bucket_key,
  approx_percentile(f.available_lots, 0.50) as median_available_lots,
  approx_percentile(f.available_lots, 0.25) as p25_available_lots,
  count(distinct f.date_key)                as n_days,
  count(*)                                  as n_obs
from {{ ref('fct_carpark_availability') }} f
join {{ ref('dim_date') }} d on f.date_key = d.date_key
where d.date >= date_add('day', -{{ var('baseline_lookback_days', 56) }}, current_date)
group by 1, 2, 3, 4, 5
```

> **⚠️ Athena has no exact `median()`.** Use `approx_percentile`, which is accurate enough and much cheaper. `n_days` (not `n_obs`) is the honest confidence signal, because the 6 polls inside one bucket on one day are near-duplicates.

### Other marts

| Mart | Grain | Purpose |
|---|---|---|
| `mart_carpark_locations` | carpark (current version only, lat/lon not null) | Carpark list + coordinates for the app's distance search |
| `mart_hourly_availability` | carpark × lot type × date × hour | Min/avg/max lots per hour, for analysis and charts. Occupancy % isn't possible because the API gives no lot totals |
| `mart_baseline_backtest` *(stretch)* | carpark × lot type × date (last 7 days) | MAE of the baseline (trained on data *before* the test week) vs. a naive "lots 30 min earlier" forecast; this proves the model's value |

---

## Serving Extracts (written by `export_serving`)

| File | Source | Columns |
|---|---|---|
| `serving/baseline.parquet` | `mart_availability_baseline` | `carpark_id, agency, lot_type, day_type, time_bucket_key, median_available_lots, p25_available_lots, n_days` |
| `serving/carparks.parquet` | `mart_carpark_locations` | `carpark_id, agency, development, area, latitude, longitude` |
| `serving/holidays.parquet` | `sg_public_holidays` seed | `date, holiday_name` (so the app can work out the arrival day type) |
| `serving/manifest.json` | export task | `generated_at_sgt, history_start_date, history_end_date, n_days_history, row_counts` |

Each file is written as **one S3 object**. A `PUT` replaces the object atomically, so the app never reads a half-written extract.

---

## Data Quality Rules

### Great Expectations: bronze, per hourly window (`quality/bronze_suite.py`)

| Expectation | Column(s) | Severity | Catches |
|---|---|---|---|
| `ExpectColumnValuesToNotBeNull` | `carpark_id`, `agency`, `lot_type`, `available_lots`, `polled_at_utc` | Critical | Schema drift, broken parsing |
| `ExpectColumnValuesToBeBetween(min_value=0)` | `available_lots` | Critical | Bad values |
| `ExpectColumnValuesToBeInSet(['C','H','Y'])` | `lot_type` | Critical | New/renamed lot types |
| `ExpectColumnValuesToBeInSet(['HDB','LTA','URA'])` | `agency` | Critical | New agencies |
| `ExpectCompoundColumnsToBeUnique` | `carpark_id, agency, lot_type, poll_id` | Critical | Duplicate pages from pagination bugs |
| `ExpectTableRowCountToBeBetween` | (table) | Critical | Silently truncated pagination; calibrate the range from week 1 (≈ polls/hour × records/poll ± 15%) |
| `ExpectColumnValuesToMatchRegex(r'^\d+\.\d+ \d+\.\d+$', mostly=0.95)` | `location` | Warning | Location format changes |
| `ExpectColumnUniqueValueCountToBeBetween(min_value=10, max_value=13)` | `poll_id` | Warning | Fewer polls than expected in the hour (12 expected) |

Implement severity as **two expectation suites** (`bronze_critical`, `bronze_warning`) in one checkpoint. A failure in the critical suite fails the Airflow task, which blocks dbt for that window and sends an alert. Results from the warning suite are logged and kept in Data Docs only.

### dbt tests (models)

| Model | Tests |
|---|---|
| `sources.yml` (bronze) | `freshness`: warn after 30 min, error after 2 h (`loaded_at_field: polled_at_utc`). Add a freshness `filter` on `dt` (e.g. last 2 days) so the check doesn't scan all of bronze |
| `stg_carpark_availability` | `not_null` on keys; `dbt_utils.unique_combination_of_columns` on the unique key; `accepted_values` on `lot_type`, `agency`; `dbt_utils.expression_is_true: available_lots >= 0` |
| `dim_carpark` | `unique` + `not_null` on `carpark_key`; singular test: at most one `is_current` row per carpark |
| `dim_date`, `dim_time_bucket` | `unique` + `not_null` on keys; `dim_time_bucket` has exactly 48 rows |
| `fct_carpark_availability` | `unique` on `availability_key`; `relationships` from each FK to its dim |
| `mart_availability_baseline` | `unique_combination_of_columns` on the grain; `accepted_values` on `day_type`; `n_days >= 1` |

---

## Storage Tiering Decision (Hot / Warm / Cold)

| Option | Decision | Reason |
|---|---|---|
| Move old bronze to S3 Standard-IA | **Deferred** | Each poll file is small (< 128 KB). Standard-IA bills a 128 KB minimum per object, and S3 lifecycle skips objects under 128 KB by default, so tiering would save nothing |
| Compact bronze monthly into larger files, then tier | Stretch | Only worthwhile once bronze grows to many GB |
| Athena results bucket | **Expire after 7 days** | Query results are disposable |

Total lake size stays in the low single-digit GB for months, which costs cents.
