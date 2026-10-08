# SG Carpark Availability Predictor — Overview & Requirements

## Document Map

| # | Document | What It Covers |
|---|---|---|
| 01 | **Overview & Requirements** (this doc) | Problem, stakeholders, requirements, scope, key decisions |
| 02 | Tech Stack & Project Structure | Every technology, how it's used, repo layout, config templates |
| 03 | Data Model & Data Quality | S3 zones, bronze/silver/gold tables, star schema, quality rules |
| 04 | Pipeline Components & Operations | Each component's logic, Airflow DAG, IAM matrix, failure handling |
| 05 | Implementation Plan | Step-by-step phases, checklists, definition of done, timeline |

---

## Project Summary

An end-to-end AWS data pipeline that **builds its own historical dataset** of Singapore carpark availability by polling the LTA DataMall API every 5 minutes, models it into a Kimball star schema with dbt, and serves a Streamlit app that answers:

> *"I'm driving to X and will arrive in ~30 minutes. Which nearby carparks are most likely to have space when I get there?"*

**Key framing:** LTA only publishes *current* availability, with no history. The pipeline is the dataset. Every 5-minute poll that is missed is lost forever, which shapes almost every design decision below.

**Prediction method:** a historical baseline that returns the median available lots per carpark × lot type × day type (weekday vs weekend/public holiday) × 30-minute time bucket. This is a legitimate, explainable baseline model, and its accuracy is measured with a backtest (see 03, `mart_baseline_backtest`).

---

## Stakeholders

### Upstream (data providers)

| Stakeholder | What They Provide | Constraints |
|---|---|---|
| LTA DataMall | `CarParkAvailabilityv2` API: HDB, LTA and URA carparks, refreshed about every minute | External, no SLA, returns 500 records per call (paginate with `$skip`), API key required |
| OneMap (Singapore Land Authority) | Search API that turns a destination into lat/lon | External; register for a token for higher rate limits |
| Ministry of Manpower (MOM) public holiday list | Public holiday dates (loaded once per year as a dbt seed) | Manual yearly update |

### Downstream (data consumers)

| Stakeholder | What They Need |
|---|---|
| Drivers (app users) | A ranked list of nearby carparks with expected availability at arrival time, in under 2 seconds |
| You (analyst / maintainer) | Queryable history in Athena, trustworthy models, alerts when something breaks |
| Portfolio reviewers / hiring managers | A clear architecture, documented trade-offs, and a working demo |

---

## Translating Needs to Requirements (EPAS)

| Prompt | Answer |
|---|---|
| **E**xisting systems | LTA's feed (and most apps built on it) shows *current* availability only, with no stored history to forecast from |
| **P**ain points | Current availability is stale by the time you arrive, so popular carparks (e.g. malls at lunch) fill up while you drive |
| **A**ctions stakeholders take | Choose a carpark *before* leaving, or pick a backup on the way |
| **S**takeholders to involve | LTA (data), OneMap (geocoding), you as operator, reviewers |

---

## Requirements

### Business Requirements

| ID | Requirement |
|---|---|
| BR-1 | Demonstrate production-style data engineering on AWS, covering ingestion, storage, modelling, orchestration, quality and serving |
| BR-2 | Keep running cost low enough to leave the pipeline on for months (target < US$50/month, < US$10 without always-on Airflow) |

### Stakeholder Requirements

| ID | Requirement | Stakeholder |
|---|---|---|
| SR-1 | Enter a destination and arrival time, get ranked nearby carparks with expected lots | Driver |
| SR-2 | See how confident the prediction is (how many days of history it is based on) | Driver |
| SR-3 | Query raw and modelled history with SQL | Analyst |
| SR-4 | Be alerted when ingestion stops or data quality fails | Operator |

### System Requirements — Functional

| ID | The system must… |
|---|---|
| FR-1 | Poll `CarParkAvailabilityv2` every 5 minutes, 24/7, reading every page |
| FR-2 | Land each poll unchanged (raw) in S3, partitioned by date and hour |
| FR-3 | Quarantine malformed records instead of dropping them silently |
| FR-4 | Validate each hour of raw data before it is transformed |
| FR-5 | Build a star schema: `dim_carpark` (SCD2), `dim_date`, `dim_time_bucket`, `fct_carpark_availability` (5-min grain) |
| FR-6 | Build a baseline mart: median available lots per carpark × lot type × day type × 30-min bucket |
| FR-7 | Refresh models hourly and publish a small serving extract |
| FR-8 | Geocode a destination, find carparks within a radius, and rank by expected availability at the arrival bucket |
| FR-9 | Alert by email (SNS) on ingestion gaps, Lambda failures, quality failures and DAG failures |

### System Requirements — Non-Functional

| ID | Requirement | How It's Met |
|---|---|---|
| NFR-1 | **Completeness:** ≥ 99% of expected polls land (≥ 285 of 288 per day) | Serverless ingestion separate from the orchestrator; retries; dead-letter queue; "no data" alarm |
| NFR-2 | **Freshness:** serving extract ≤ 2 hours old | Hourly DAG; app shows a warning banner if the manifest is stale |
| NFR-3 | **Latency:** app responds in < 2 s | App reads a cached Parquet extract and never runs Athena per request |
| NFR-4 | **Cost:** see BR-2 | Pay-per-use services (Lambda, Athena, S3); Athena scan cutoff; budget alert |
| NFR-5 | **Security:** least privilege, no secrets in code | One IAM role per component; key in SSM Parameter Store; GitHub OIDC; no inbound ports on EC2 |
| NFR-6 | **Reproducibility:** whole stack rebuildable from the repo | Terraform for all infrastructure; dbt for all transforms; bronze is replayable |
| NFR-7 | **Idempotency:** re-running any hour gives the same result | Window-based processing + Iceberg `merge` on unique keys |

---

## Scope

| In Scope | Out of Scope |
|---|---|
| All HDB / LTA / URA carparks returned by the API | Live traffic or travel-time estimation (user supplies arrival time) |
| Lot types returned by the API (car, motorcycle, heavy vehicle) | Total capacity or occupancy % (the API doesn't provide lot totals) |
| Baseline (median) forecast + backtest | ML models (stretch goal once ≥ 8 weeks of history exist) |
| Streamlit web app | Mobile app, user accounts, payments |

---

## Key Design Decisions

| # | Decision | Choice | Alternatives Considered | Why |
|---|---|---|---|---|
| D1 | Ingestion pattern | **Batch on a fixed interval** (EventBridge Scheduler → Lambda, every 5 min) | Kinesis Data Streams, MSK | Source is an API you poll, not producers pushing events; 5-min freshness meets every requirement; streaming adds cost without improving the forecast |
| D2 | Where ingestion runs | **Outside Airflow** | Airflow task every 5 min | Missed polls can never be recovered. Airflow downtime must not create gaps. Everything downstream can be replayed from bronze |
| D3 | ETL vs ELT | **ELT**: land raw, transform with dbt on Athena | Glue ETL jobs | Raw data is preserved for replays; SQL transforms are testable and versioned |
| D4 | Storage abstraction | **Lakehouse**: S3 + Iceberg, medallion zones | Redshift warehouse | Pay-per-query; ACID merges for incremental loads and SCD2; open format |
| D5 | Processing engine | **Athena (Trino)** | Spark / EMR | Hundreds of thousands of rows/day is small; distributed processing is unnecessary |
| D6 | Modelling approach | **Kimball star schema** | Inmon, Data Vault | One source, one well-defined use case, so fast iteration beats enterprise normalisation |
| D7 | Partition timezone | **UTC in bronze, SGT from silver onward** | SGT everywhere | UTC raw storage is standard and avoids projection edge cases; business logic (day type, buckets) needs local time |
| D8 | Serving | **Precomputed Parquet extract → Streamlit cache** | Query Athena per request | Sub-second responses, near-zero query cost, no Athena credentials in the app |
| D9 | Proximity | **Computed in the app per request** (haversine) | Precomputed proximity mart | Destinations are free text, so they can't be precomputed |
| D10 | Orchestrator | **Airflow 3 on EC2 (Docker, LocalExecutor)**; MVP uses a GitHub Actions cron | MWAA | MWAA is costly for a portfolio project; Airflow 2 reached end of life in April 2026 |

---

## Success Criteria

| Area | Metric | Target |
|---|---|---|
| Ingestion | Polls landed / expected polls (daily) | ≥ 99% |
| Quality | GX + dbt tests passing on scheduled runs | 100% (failures alert and block) |
| Freshness | Age of serving extract | ≤ 2 h |
| App | p95 response time | < 2 s |
| Forecast | Backtest MAE of baseline vs. naive "current availability" | Baseline beats naive at 30-min horizon for most carparks |
| Cost | Monthly AWS bill | Within BR-2 |

---

## Glossary

| Term | Meaning |
|---|---|
| Poll | One full paginated read of the API (all pages), every 5 minutes |
| Snapshot slot | A poll timestamp floored to the 5-minute boundary (SGT) |
| Time bucket | One of 48 half-hour buckets in a day (0 = 00:00–00:29, 47 = 23:30–23:59) |
| Day type | `weekday` or `weekend_holiday` (public holidays are grouped with weekends) |
| Window | The hour of data a DAG run processes: `[window_start_utc, window_end_utc)` |
