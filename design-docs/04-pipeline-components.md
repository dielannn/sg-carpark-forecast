# SG Carpark Availability Predictor — Pipeline Components & Operations

## Component Map

```
ALWAYS ON (every 5 min)            HOURLY (Airflow, :15 past)          ON REQUEST (user)
───────────────────────            ──────────────────────────          ─────────────────
EventBridge Scheduler              compute_window                      Streamlit app
Ingestion Lambda                   check_bronze_partitions               └─ OneMap geocode
CloudWatch metrics + alarms        validate_bronze_gx                    └─ distance + ranking
                                   dbt_build_silver
                                   dbt_build_gold
                                   export_serving

CROSS-CUTTING
─────────────
Terraform · IAM roles · SSM Parameter Store · SNS alerts · GitHub Actions CI/CD · AWS Budgets
```

---

## Component Catalog

Each component shows its trigger, what it does, what it reads and writes, and which IAM principal it runs as.

| Component | Trigger | What Happens | Reads | Writes | Runs As |
|---|---|---|---|---|---|
| EventBridge Scheduler `carpark-poll` | `rate(5 minutes)` | Invokes the ingestion Lambda asynchronously | — | — | `scheduler-invoke-role` |
| Lambda `carpark-ingest` | Scheduler | Paginates the API, validates, writes Parquet + quarantine, emits metrics | LTA API, SSM | `bronze/`, `quarantine/`, CloudWatch | `lambda-ingest-role` |
| Airflow DAG `carpark_transform` | Cron `15 * * * *` (SGT) | Validates the hour, runs dbt, exports serving files | `bronze/`, Glue, Athena | `silver/`, `gold/`, `serving/` | `airflow-ec2-role` (instance profile) |
| Streamlit app | User request | Geocodes the destination, ranks carparks | `serving/`, OneMap | — | `streamlit-reader` IAM user |
| GitHub Actions `ci.yml` | Pull request | Lint, unit tests, `terraform plan`, `dbt build` (dev) | Repo, AWS (read) | `dev/` | `github-deploy-role` (OIDC) |
| GitHub Actions `deploy.yml` | Push to `main` | `terraform apply`, deploy Lambda zip | Repo | AWS resources | `github-deploy-role` (OIDC) |
| GitHub Actions `dbt_hourly.yml` | Cron (MVP only) | Same as DAG steps 4–6, without GX | `bronze/` | `silver/`, `gold/`, `serving/` | `github-dbt-role` (OIDC; same permissions as the Airflow role, deleted after Phase 5) |

---

## 1. Ingestion (`ingestion/lambda_function.py`)

### Configuration

| Setting | Value |
|---|---|
| Runtime / arch | `python3.12` / `x86_64` |
| Memory / timeout | 512 MB / 60 s |
| Layers | AWS SDK for pandas (`AWSSDKPandas-Python312`) |
| Env vars | `LAKE_BUCKET`, `LTA_KEY_PARAM=/carpark/lta_account_key`, `LTA_BASE_URL` |
| Async invoke config | `maximum_retry_attempts = 2`, `maximum_event_age_in_seconds = 240`, `on_failure` destination → SQS `carpark-ingest-dlq` |
| Scheduler target | `retry_policy { maximum_retry_attempts = 2, maximum_event_age_in_seconds = 240 }`, `dead_letter_config` → same SQS queue |
| VPC | **None**, so no NAT gateway is needed |

### Handler Logic

```
Ingest (every 5 min):
  1. polled_at_utc = now (UTC, truncated to seconds); poll_id = polled_at_utc as 'YYYYMMDDTHHMMSSZ'
  2. api_key = SSM GetParameter(LTA_KEY_PARAM, WithDecryption) — cache in a module global with a 1-hour TTL
  3. session = requests.Session(); headers = { AccountKey: api_key, accept: application/json }
     mount HTTPAdapter with urllib3 Retry(total=3, backoff_factor=1, status_forcelist=[429,500,502,503,504])
  4. skip = 0; records = []
     loop (max 20 pages as a safety guard):
        resp = session.get(LTA_BASE_URL, params={'$skip': skip}, timeout=10); resp.raise_for_status()
        page = resp.json()['value']; records += page
        if len(page) < 500: break
        skip += 500
  5. If records is empty → raise (an empty poll is a failure, not a success)
  6. For each record: required keys present AND AvailableLots castable to int → good, else → bad (+ reason)
  7. df = DataFrame(good) → rename to snake_case → cast types → add polled_at_utc, poll_id
  8. wr.s3.to_parquet(df, path=f"s3://{LAKE_BUCKET}/bronze/carpark_availability/dt={YYYY-MM-DD}/hour={HH}/poll_{poll_id}.parquet")
  9. If bad: write JSON lines to quarantine/carpark_availability/dt=…/poll_{poll_id}.json
 10. cloudwatch.put_metric_data(Namespace='Carpark', RecordsIngested=len(good), RecordsQuarantined=len(bad), Pages=n)
 11. Log one structured summary line; return it
  Any unhandled exception → Lambda async retry (×2) → SQS DLQ → alarm
```

> **⚠️ `requests` is not in the Lambda Python runtime.** Bundle it in the deployment zip (`pip install -t`). The AWS SDK for pandas layer provides pandas, pyarrow and awswrangler. The layer must match both the runtime version and the CPU architecture.

> **⚠️ Two retry layers, two failure types.** The Scheduler's DLQ catches *delivery* failures (e.g. a permissions issue invoking Lambda). Lambda's async `on_failure` destination catches *function* errors. You need both.

> **⚠️ Don't set reserved concurrency** on a new AWS account. New accounts often have a low concurrency quota, and AWS won't let you reserve below the unreserved minimum.

---

## 2. Orchestration (`orchestration/dags/carpark_transform.py`)

### DAG Settings

| Setting | Value | Why |
|---|---|---|
| `dag_id` | `carpark_transform` | |
| `schedule` | `"15 * * * *"` | 15 minutes past the hour, so the last poll of the previous hour (:55) has landed |
| `start_date` | `pendulum.datetime(<go-live date>, tz="Asia/Singapore")` | Timezone-aware |
| `catchup` | `True` | After Airflow downtime, missed hours are processed automatically |
| `max_active_runs` | `1` | Catch-up runs execute one at a time, in order |
| `default_args` | `retries=1`, `retry_delay=5 min`, `on_failure_callback=notify_sns` | |
| `tags` | `["carpark", "transform"]` | |

### Task Catalog (in execution order)

| # | `task_id` | Type | What Happens | Fails When |
|---|---|---|---|---|
| 1 | `compute_window` | `@task` | `window_end_utc = logical_date (UTC) floored to the hour`; `window_start_utc = window_end_utc − 1 h`. Returns a dict (XCom) | — |
| 2 | `check_bronze_partitions` | `@task` | Lists objects under `bronze/…/dt=…/hour=…/` for the window; logs the count | 0 files (warns if < 10 of 12) |
| 3 | `validate_bronze_gx` | `@task` | `wr.s3.read_parquet` the hour → pandas → GX checkpoint with `bronze_critical` + `bronze_warning` suites | Any critical expectation fails |
| 4 | `dbt_build_silver` | `BashOperator` | `dbt source freshness && dbt build --select tag:silver --vars '{window_start_utc: …, window_end_utc: …}'` (seed, stg, snapshot + tests). `dbt build` doesn't run source freshness, so it's called explicitly | Stale source, model error or test failure |
| 5 | `dbt_build_gold` | `BashOperator` | `dbt build --select tag:gold --vars …` (dims, fact, marts + tests) | Model error or test failure |
| 6 | `export_serving` | `@task` | Reads marts via `wr.athena.read_sql_query` → writes `serving/*.parquet` + `manifest.json` | Empty result or write error |

**Dependencies:**

```python
window = compute_window()
check_bronze_partitions(window) >> validate_bronze_gx(window) >> dbt_build_silver >> dbt_build_gold >> export_serving()
```

### Skeleton (Airflow 3 syntax)

```python
import pendulum
from airflow.sdk import dag, task, Variable
from airflow.providers.standard.operators.bash import BashOperator

DBT = "/opt/dbt_venv/bin/dbt"
DBT_DIR = "/opt/airflow/dbt"

@dag(
    dag_id="carpark_transform",
    schedule="15 * * * *",
    start_date=pendulum.datetime(2026, 10, 19, tz="Asia/Singapore"),
    catchup=True,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=5),
                  "on_failure_callback": notify_sns},   # defined in utils/alerts.py
    tags=["carpark", "transform"],
)
def carpark_transform():
    @task
    def compute_window(logical_date=None, dag_run=None) -> dict:
        # Manual triggers in Airflow 3 can have logical_date=None, so fall back to run_after
        ts = pendulum.instance(logical_date or dag_run.run_after)
        end = ts.in_timezone("UTC").start_of("hour")
        start = end.subtract(hours=1)
        return {"window_start_utc": start.to_datetime_string(),
                "window_end_utc": end.to_datetime_string()}
    ...

carpark_transform()
```

> **⚠️ Airflow 3 differs from the course notes (Airflow 2):**
> 1. Imports: `from airflow.sdk import dag, task, Variable`; operators come from `airflow.providers.standard.operators.*`.
> 2. `catchup` now defaults to `False`, so set it explicitly.
> 3. `schedule_interval` is gone; use `schedule`.
> 4. Cron schedules no longer create data intervals by default (`data_interval_start == data_interval_end`). That's why `compute_window` derives the window from `logical_date` instead of `data_interval_start`.
> 5. Manually triggered runs can have `logical_date = None`; `compute_window` falls back to `dag_run.run_after`.

> **⚠️ Best practices from the course still apply:** no top-level code (DAG files are re-parsed constantly), bucket names in Airflow Variables (`lake_bucket`, `sns_topic_arn`), and XCom for the small window dict only. Data always stays in S3.

### Airflow Host

| Item | Setting |
|---|---|
| Instance | `t3.medium` (4 GB), Amazon Linux 2023, 30 GB gp3, 2 GB swap file |
| Network | Custom VPC `10.0.0.0/16` → public subnet `10.0.1.0/24` → route `0.0.0.0/0` to the IGW; security group with **no inbound rules** |
| Access | `aws ssm start-session --target <id> --document-name AWS-StartPortForwardingSession --parameters '{"portNumber":["8080"],"localPortNumber":["8080"]}'` → open `localhost:8080` |
| Instance metadata | IMDSv2 required, **`http_put_response_hop_limit = 2`** (Docker containers need the extra hop to get instance-profile credentials) |
| Compose services | `postgres`, `airflow-apiserver`, `airflow-scheduler`, `airflow-dag-processor`, `airflow-init` (from the official compose file, with Celery/Redis/worker removed and `AIRFLOW__CORE__EXECUTOR=LocalExecutor`) |
| Image | `FROM apache/airflow:3.x` (pin the version) + `pip install -r requirements.txt` + `python -m venv /opt/dbt_venv && /opt/dbt_venv/bin/pip install -r dbt/requirements.txt` |
| Startup | `restart: unless-stopped`; a systemd unit runs `docker compose up -d` on boot |

> **⚠️ Install dbt in its own virtualenv.** dbt and Airflow pin conflicting dependencies. If GX also conflicts, run `validate_bronze_gx` with `@task.external_python` pointing to a third venv.

---

## 3. dbt Run Order

> **⚠️ Order matters.** `dbt build` resolves dependencies inside a selection, but the silver → gold split across two Airflow tasks must respect this order:
> 1. `sg_public_holidays` (seed, tag `silver`)
> 2. `stg_carpark_availability` (tag `silver`)
> 3. `snap_carpark` (snapshot, tag `silver`, depends on stg)
> 4. `dim_date`, `dim_time_bucket`, `dim_carpark` (tag `gold`, depend on seed / snapshot)
> 5. `fct_carpark_availability` (tag `gold`, depends on stg + all dims)
> 6. `mart_*` (tag `gold`, depend on fact + dims)
>
> If gold runs before the snapshot has captured a new carpark, that carpark's facts fail the `relationships` test.

**Schema naming:** override `generate_schema_name` so that in `prod`, `+schema: gold` builds into `gold` (not `silver_gold`). In `dev`, it builds into `dev_silver` / `dev_gold`.

**Selectors used by each runner:**

| Runner | Command |
|---|---|
| Airflow (hourly) | `dbt build --select tag:silver …` then `dbt build --select tag:gold …` |
| GitHub Actions CI (PR) | `dbt build --target dev --select state:modified+ --defer --state prod-manifest/`; fall back to a full dev build on the first runs |
| Manual full rebuild | `dbt build --full-refresh --select stg_carpark_availability+` (replays all of bronze) |

---

## 4. Serving App (`app/streamlit_app.py`)

### Request Flow

```
On load:
  baseline, carparks, holidays, manifest = load_serving()   # @st.cache_data(ttl=3600); boto3 get_object → pd.read_parquet
  If manifest.generated_at_sgt is more than 2 h old → st.warning("Predictions may be stale")

User inputs: destination (text), arrival time (default now + 30 min, SGT), lot type (default 'C'), radius (default 1 km)

On submit:
  1. Geocode: GET https://www.onemap.gov.sg/api/common/v2.1/search?searchVal=<text>&returnGeom=Y&getAddrDetails=Y
     → first result's LATITUDE/LONGITUDE (@st.cache_data on the query string)
     No result → st.error("Couldn't find that place — try a postal code")
  2. Distance: vectorised haversine from destination to every row in carparks → keep distance ≤ radius
     None in radius → widen to 2 km once, then tell the user
  3. Arrival bucket = hour * 2 + (minute >= 30); day_type = 'weekend_holiday' if weekend or date in holidays else 'weekday'
  4. Join nearby carparks to baseline on (carpark_id, agency, lot_type, day_type, time_bucket_key)
  5. Rank: median_available_lots desc, then distance asc
     Confidence label from n_days: ≥ 6 High · 3–5 Medium · 1–2 Low · missing "No history yet"
  6. Render: ranked table (development, distance, typical lots, p25 "worst case", confidence) + pydeck map
     Footer: "Based on {n_days_history} days of history · updated {generated_at_sgt}"
```

> **⚠️ Wording matters:** label the number "typical lots available at this time", not a guarantee. The p25 column shows a pessimistic case.

---

## 5. IAM Permission Matrix

| Permission | Lambda ingest | Scheduler | Airflow EC2 | Streamlit user | GitHub deploy |
|---|:---:|:---:|:---:|:---:|:---:|
| `s3:PutObject` on `bronze/*`, `quarantine/*` | ✅ | ❌ | ❌ | ❌ | ❌ |
| `s3:GetObject`, `s3:ListBucket` on `bronze/*` | ❌ | ❌ | ✅ | ❌ | ✅ |
| `s3:*Object` on `silver/*`, `gold/*`, `serving/*` | ❌ | ❌ | ✅ | ❌ | ❌ |
| `s3:*Object` on `dev/*` | ❌ | ❌ | ❌ | ❌ | ✅ |
| `s3:GetObject` on `serving/*` | ❌ | ❌ | ✅ | ✅ | ❌ |
| Athena query on workgroup `carpark` + results bucket | ❌ | ❌ | ✅ | ❌ | ✅ |
| Glue read: `bronze` | ❌ | ❌ | ✅ | ❌ | ✅ |
| Glue create/update/delete tables: `silver`, `gold` | ❌ | ❌ | ✅ | ❌ | ❌ |
| Glue create/update/delete: `dev_*` databases | ❌ | ❌ | ❌ | ❌ | ✅ |
| `ssm:GetParameter` on `/carpark/lta_account_key` | ✅ | ❌ | ❌ | ❌ | ❌ |
| `cloudwatch:PutMetricData` (namespace `Carpark`) | ✅ | ❌ | ❌ | ❌ | ❌ |
| `lambda:InvokeFunction` on `carpark-ingest` | ❌ | ✅ | ❌ | ❌ | ❌ |
| `sqs:SendMessage` on `carpark-ingest-dlq` | ✅ | ✅ | ❌ | ❌ | ❌ |
| `sns:Publish` on `carpark-alerts` | ❌ | ❌ | ✅ | ❌ | ❌ |
| `AmazonSSMManagedInstanceCore` (Session Manager) | ❌ | ❌ | ✅ | ❌ | ❌ |
| Terraform apply on project resources | ❌ | ❌ | ❌ | ❌ | ✅ |

- **Lambda ingest** is write-only to raw: it can't read or delete what it wrote.
- **Streamlit** credentials are the only long-lived keys. Scope them to `serving/*` and rotate them every 90 days.
- **GitHub** uses OIDC with a trust policy limited to `repo:<user>/sg-carpark-pipeline:*`, so no stored AWS keys.
- **MVP only:** `github-dbt-role` has the same permissions as the Airflow EC2 column. It's used by `dbt_hourly.yml` and removed once Airflow takes over (Phase 5).

---

## 6. Monitoring & Alerting

All alarms notify SNS topic `carpark-alerts` (email subscription; confirm the email once).

| Alarm | Metric | Condition | Catches |
|---|---|---|---|
| `ingest-no-data` | `Carpark/RecordsIngested` (Sum, 5 min) | < 1 for 3 of 3 periods, **`treat_missing_data = breaching`** | Lambda not running at all (schedule disabled, deleted, throttled) |
| `ingest-errors` | `AWS/Lambda Errors` (Sum, 5 min) | ≥ 1 for 2 of 3 periods | Repeated failures (single transient errors are retried) |
| `ingest-dlq-not-empty` | `AWS/SQS ApproximateNumberOfMessagesVisible` | > 0 | A poll that failed every retry, i.e. lost data |
| `quarantine-spike` | `Carpark/RecordsQuarantined` (Sum, 15 min) | > 100 | API schema change |
| Airflow task failure | `on_failure_callback` → `sns.publish` | Any task fails after retries | GX failure, dbt failure, export failure |
| AWS Budget | Monthly cost | 80% actual, 100% forecast | Cost surprises |

---

## 7. Failure Handling

| Situation | What Happens | Handling |
|---|---|---|
| API timeout / 5xx | In-function retries (×3, backoff) | Then Lambda async retry (×2), then DLQ + alarm |
| API key invalid / expired (401/403) | Every poll fails | `ingest-errors` alarm → rotate key in SSM (see runbook) |
| Pagination truncated (fewer pages) | Fewer rows land | GX `ExpectTableRowCountToBeBetween` fails → DAG blocks → alert |
| Malformed records | Written to `quarantine/` | `quarantine-spike` alarm if many; inspect and fix parsing |
| Duplicate polls (retries) | Two polls in one 5-min slot | Deduplicated in `stg` on the unique key |
| GX critical failure | `validate_bronze_gx` fails; dbt doesn't run for that hour | Investigate, then clear the task in the Airflow UI to re-run |
| dbt test failure | `dbt_build_*` fails; serving keeps the last good extract | Fix the model, re-run the task (merges are idempotent) |
| Airflow host down | Bronze keeps accumulating; serving goes stale | App shows a stale banner; on restart, `catchup` processes missed hours in order |
| Serving file missing / unreadable | App can't load data | `st.error` with a friendly message; cached data is used until the TTL expires |
| OneMap unavailable | Geocoding fails | `st.error`; suggest entering a postal code; retry later |

---

## 8. Runbook

| Task | Steps |
|---|---|
| **Re-run one hour** | Airflow UI → DAG run → select the failed task → Clear (with downstream). It reuses the same window. |
| **Catch up after downtime** | Start the EC2 instance; `catchup=True` schedules every missed hour. To reprocess a specific range on demand, use Airflow 3 backfill (UI, or `airflow backfill create --dag-id carpark_transform --from-date … --to-date …`). |
| **Rebuild after a model logic change** | `dbt build --full-refresh --select stg_carpark_availability+`, then trigger `export_serving` |
| **Rotate the LTA key** | `aws ssm put-parameter --name /carpark/lta_account_key --type SecureString --value <new> --overwrite`. The Lambda picks it up within its 1-hour cache TTL (or immediately: update any env var to force a cold start). |
| **Pause ingestion** | Set `ingestion_enabled = false` in `terraform.tfvars` → `terraform apply` (sets the schedule `state = "DISABLED"`) |
| **Yearly holiday update** | Add next year's MOM holidays to `seeds/sg_public_holidays.csv` and extend the `dim_date` spine every December |
| **Drain the DLQ** | Inspect messages (they show which polls were lost), record the gap in the README data log, then purge |

---

## 9. Cost Estimate (ap-southeast-1)

Rough monthly figures. Verify with the AWS Pricing Calculator before relying on them.

| Service | Usage | Est. US$/month |
|---|---|---|
| Lambda | ~8,640 invocations × ~5 s × 512 MB | ~0 (within the free monthly allowance) |
| EventBridge Scheduler | ~8,640 invocations | ~0 |
| S3 | < 5 GB + ~10K PUTs | < 1 |
| Athena | Hourly runs scanning a few hundred MB, plus ad-hoc queries | 1–5 |
| Glue Data Catalog | A handful of tables | ~0 |
| CloudWatch | 2 custom metrics, ~5 alarms, logs | 1–3 |
| SQS / SNS | Negligible | ~0 |
| EC2 `t3.medium` + 30 GB gp3 + public IPv4 address (24/7) | Airflow host (AWS charges hourly for public IPv4 addresses) | ~40–45 |
| Streamlit Community Cloud | — | 0 |
| **Total** | | **~40–50 with Airflow always on; ~3–8 during the MVP phase** |

**Savings lever:** stopping the Airflow host overnight costs no data, because ingestion is independent and `catchup` reprocesses the missed hours on restart.
