# SG Carpark Availability Predictor — Implementation Plan

## How to Use This Plan

Work through the phases in order. Each phase has:
- **Goal:** what is true when it's finished
- **Steps:** checkboxes to tick off, in order
- **Definition of Done (DoD):** how you *prove* it works
- **Gotchas:** known traps

> **⚠️ The critical path is ingestion.** History can't be backfilled, so every day ingestion isn't live is a day of data you'll never have. Get Phases 1a–1b (Terraform foundations + Lambda) live as early as you can. If you can only front-load extra hours in one place, put them here. Monitoring (1c) and everything else are built while data accumulates.

---

## Timeline Overview (at ~10 hours/week)

| Phase | Goal | Est. Hours | Weeks | Milestone |
|---|---|---|---|---|
| 0 | Accounts, tools, API key | 2–4 | 1 | |
| 1 | Ingestion live + monitored | 17–27 | 1–3 | 🟢 **Data flowing** (after 1b) |
| 2 | Catalog + data profiling | 2–4 | 3 | |
| 3 | dbt models + tests | 20–30 | 3–6 | |
| 4 | Streamlit app + MVP scheduler | 14–21 | 6–7 | 🚀 **MVP live** |
| 5 | Airflow + Great Expectations | 14–23 | 7–9 | |
| 6 | CI/CD + security hardening | 4–6 | 9–10 | |
| 7 | Backtest, README, demo | 5–8 | 10 | 🏁 **Portfolio-ready** |
| | **Total** | **~80–120** | **~8–12 weeks** | |

At ~20 hrs/week this compresses to 4–6 weeks; full-time, 2–3 weeks.

### What the Data Lets You Show Over Time

Counted from the day ingestion goes live, not from the start of the project.

| After | History | What You Can Show |
|---|---|---|
| Day 1 | ~0.6–0.9M rows | Ingestion works; data profile |
| Week 1 | ~5 weekdays, 2 weekend days | Weekday predictions (low–medium confidence) |
| Weeks 3–4 | 6+ days for every day type | Confident predictions for most buckets, so the data is MVP-ready (lines up with Phase 4) |
| Week 8 | Full 56-day lookback | Meaningful backtest; ML stretch becomes possible |

---

## Phase 0 — Prerequisites (things to do outside code)

**Goal:** every account, key and tool is ready before you write any code.

- [ ] **LTA DataMall API key:** request API access on the LTA DataMall site; the key arrives by email
- [ ] Test the key:
  ```bash
  curl -H "AccountKey: $LTA_KEY" -H "accept: application/json" \
    "https://datamall2.mytransport.sg/ltaodataservice/CarParkAvailabilityv2?\$skip=0"
  ```
- [ ] **AWS account hygiene:**
  - [ ] MFA on the root user; stop using root after this
  - [ ] Create an admin user via IAM Identity Center (or an IAM user with MFA)
  - [ ] Create one **AWS Budget** by hand now (e.g. US$20) as a safety net; Terraform manages the real one later
  - [ ] Set default region `ap-southeast-1`
- [ ] **Install locally:** AWS CLI v2 + **Session Manager plugin**, Terraform ≥ 1.11, Python 3.12, Docker Desktop, Git
- [ ] Configure a CLI profile (`aws configure sso` → profile `carpark`), then `export AWS_PROFILE=carpark`
- [ ] **GitHub:** create public repo `sg-carpark-pipeline`, add `.gitignore` (see 02), protect `main`, and copy these docs into `docs/`
- [ ] **Streamlit Community Cloud:** sign in with GitHub
- [ ] **OneMap:** register for an API token (optional, for higher rate limits)
- [ ] Pick a **go-live date** for ingestion (target: end of week 1)

**DoD:** the curl returns JSON with a `value` array of 500 records, `aws sts get-caller-identity` works with your profile, and the repo exists.

---

## Phase 1 — Foundations + Ingestion Live

**Goal:** a poll file lands in S3 every 5 minutes, and you get an email when it stops.

### 1a. Terraform foundations

- [ ] `infra/bootstrap/`: state bucket `carpark-tfstate-<acct>` (versioning, SSE-S3, block public access) with **local** state → `terraform init && terraform apply`
- [ ] `infra/versions.tf`: `required_version`, AWS provider `~> 6.0`, `backend "s3"` with `use_lockfile = true`
- [ ] `infra/providers.tf`: region + `default_tags { project = "carpark", managed_by = "terraform" }`; **no credentials in the block**
- [ ] `infra/s3.tf`: `carpark-lake-<acct>` and `carpark-athena-results-<acct>` buckets (block public access, SSE-S3); lifecycle rule expiring Athena results after 7 days
- [ ] `infra/ssm.tf`: `aws_ssm_parameter` `/carpark/lta_account_key` (`SecureString`, placeholder value, `lifecycle { ignore_changes = [value] }`)
- [ ] `terraform apply`, then set the real key with `aws ssm put-parameter … --overwrite` (see 02)

### 1b. Ingestion Lambda

- [ ] Write `ingestion/lambda_function.py` following the handler logic in 04
- [ ] Unit tests (`pytest` + `unittest.mock`):
  - [ ] Pagination stops when a page returns < 500 records
  - [ ] Bad records go to quarantine with a reason
  - [ ] An empty response raises
  - [ ] Output column names and types match the bronze table (03)
- [ ] Run the handler locally once against the real API, writing to a local file, and inspect it with pandas
- [ ] `ingestion/build.sh`: `pip install -r requirements.txt -t build/ && cp lambda_function.py build/ && zip`
- [ ] `infra/iam.tf`: `lambda-ingest-role`, `scheduler-invoke-role` (permissions per the IAM matrix in 04)
- [ ] `infra/lambda.tf`: function (`source_code_hash` from the zip), AWS SDK for pandas layer ARN as a variable, env vars, log group with 14-day retention, `aws_lambda_function_event_invoke_config` (retries + `on_failure` → SQS)
- [ ] `infra/scheduler.tf`: SQS `carpark-ingest-dlq`; `aws_scheduler_schedule` with `rate(5 minutes)`, `flexible_time_window { mode = "OFF" }`, retry policy, DLQ, and `state` driven by `var.ingestion_enabled`
- [ ] `terraform apply` → watch CloudWatch Logs → confirm files appear under `bronze/…/dt=…/hour=…/` 🟢

### 1c. Monitoring

- [ ] `infra/monitoring.tf`: SNS topic `carpark-alerts` + email subscription (**click the confirmation email**)
- [ ] The four ingestion alarms from 04 §6 (remember `treat_missing_data = "breaching"` on `ingest-no-data`)
- [ ] `aws_budgets_budget` with 80% actual / 100% forecast notifications

**DoD:**
- [ ] 24 hours of data: ~288 poll files in bronze for one UTC day
- [ ] **Alarm test 1:** set `ingestion_enabled = false`, apply, wait 20 min → `ingest-no-data` email arrives; re-enable
- [ ] **Alarm test 2:** temporarily point `LTA_BASE_URL` at a wrong path → `ingest-errors` and DLQ alarms fire; revert
- [ ] `git grep` finds no keys or secrets in the repo

**Gotchas:**
- The AWS SDK for pandas layer ARN is region-, runtime- and architecture-specific. Copy it from the official docs list.
- Each alarm test leaves a gap in bronze. Note it in a `docs/data-log.md` so the gap is explained later.
- New accounts may hit a low Lambda concurrency quota. Don't set reserved concurrency.

---

## Phase 2 — Catalog + Data Profiling

**Goal:** bronze is queryable in Athena, and you know what the data really looks like.

- [ ] `infra/glue.tf`: databases `bronze`, `silver`, `gold`; table `bronze.carpark_availability_raw` with partition projection (03)
- [ ] `infra/athena.tf`: workgroup `carpark` with a result location in the results bucket, `enforce_workgroup_configuration = true`, `bytes_scanned_cutoff_per_query` ≈ 10 GB
- [ ] Run profiling queries in the Athena console:
  - [ ] Records per poll: `select poll_id, count(*) … where dt = '<day>' group by 1`
  - [ ] Distinct `lot_type` and `agency` values and their counts
  - [ ] Rows with empty / unparseable `location`
  - [ ] Carpark IDs shared across agencies: `… group by carpark_id having count(distinct agency) > 1`
  - [ ] Min / max / percentiles of `available_lots`
- [ ] Write the findings to `docs/data-profile.md`. These numbers set your GX thresholds.

**DoD:** every profiling query runs, `data-profile.md` exists, and you have a row-count range per hour for GX.

---

## Phase 3 — dbt Models + Tests

**Goal:** `dbt build` produces the full star schema and marts with all tests passing, and re-running a window changes nothing.

- [ ] Create a venv, `pip install -r dbt/requirements.txt`, `dbt init` → move into `dbt/`
- [ ] `profiles.yml` from 02 (env vars only); `dbt debug` passes
- [ ] `packages.yml` → `dbt deps`
- [ ] `macros/generate_schema_name.sql` (prod → `silver`/`gold`; dev → `dev_silver`/`dev_gold`)
- [ ] `dbt_project.yml`:
  - [ ] `silver` folder: `+schema: silver`, `+tags: [silver]`
  - [ ] `gold` folder: `+schema: gold`, `+tags: [gold]`
  - [ ] Default `+table_type: iceberg`
  - [ ] Seeds: `+schema: silver`, `+tags: [silver]`
  - [ ] Snapshots: `+schema: silver`, `+tags: [silver]`, `+table_type: iceberg` (otherwise `--select tag:silver` skips the snapshot)
  - [ ] Vars: `baseline_lookback_days: 56` and default window vars (last 2 hours)
- [ ] `models/sources.yml`: bronze source + freshness (with `dt` filter)
- [ ] `stg_carpark_availability`: build it once with `--full-refresh` over all bronze, then test an incremental run with explicit window vars
- [ ] Seed `sg_public_holidays.csv`; build `dim_date`, `dim_time_bucket`
- [ ] Snapshot `snap_carpark`; build `dim_carpark` (with the backdated first version, see 03)
- [ ] `fct_carpark_availability` (incremental merge)
- [ ] Marts: `mart_carpark_locations`, `mart_availability_baseline`, `mart_hourly_availability`
- [ ] All tests from 03 in `_silver.yml` / `_gold.yml`, plus the singular test "one `is_current` row per carpark"
- [ ] `dbt docs generate && dbt docs serve`: screenshot the lineage graph for the README

**DoD:**
- [ ] `dbt build` passes with 0 errors and 0 test failures
- [ ] **Idempotency test:** run the same window twice; row counts in `stg` and `fct` don't change
- [ ] `mart_availability_baseline` has rows for (almost) every carpark in `mart_carpark_locations`

**Gotchas:**
- Iceberg requires `timestamp(6)`, so cast explicitly.
- Use `approx_percentile` (Athena has no `median`).
- Forgetting the window filter in an incremental model causes a full bronze scan every hour. Check "Data scanned" in the Athena query history.
- With `schema_table_unique`, every full refresh writes to a new S3 path. Old paths can be cleaned up with a lifecycle rule later.

---

## Phase 4 — Serving App + MVP Scheduler

**Goal:** a public Streamlit URL returns ranked carparks, refreshed hourly without Airflow.

### 4a. Export + app

- [ ] `orchestration/dags/utils/export.py`: reads the marts with `wr.athena.read_sql_query` and writes `serving/*.parquet` + `manifest.json` (03). Make it runnable standalone: `python -m utils.export`
- [ ] Run the export locally → confirm the files in S3
- [ ] `app/geo.py`: OneMap search client + vectorised haversine (NumPy), with unit tests on known distances
- [ ] `app/data.py`: `@st.cache_data(ttl=3600)` loaders using boto3 + `st.secrets`
- [ ] `app/streamlit_app.py`: inputs, ranking, confidence labels, pydeck map, stale banner (04 §4)
- [ ] Run locally with `streamlit run app/streamlit_app.py`

### 4b. Deploy

- [ ] Terraform: IAM user `streamlit-reader` with a policy for `s3:GetObject` on `serving/*` only
- [ ] Create its access key **with the CLI, not Terraform** (keys created in Terraform are stored in plaintext in state)
- [ ] Streamlit Community Cloud: New app → repo → `app/streamlit_app.py` → paste secrets → deploy

### 4c. MVP scheduler (GitHub Actions)

- [ ] `infra/github_oidc.tf`: OIDC provider + `github-dbt-role` (trust limited to your repo)
- [ ] `.github/workflows/dbt_hourly.yml`:
  - [ ] `on: schedule: - cron: '15 * * * *'` + `workflow_dispatch`
  - [ ] `permissions: id-token: write, contents: read`
  - [ ] Steps: checkout → setup Python → `aws-actions/configure-aws-credentials` (role-to-assume) → install dbt → compute the last full UTC hour → `dbt build` silver then gold with window vars → run the export

**DoD (🚀 MVP):**
- [ ] The public app URL works end to end for 5 test destinations (mall, MRT station, HDB town, CBD office, postal code)
- [ ] p95 response < 2 s after the first (cached) load
- [ ] The hourly workflow has run green 24 times in a row; `manifest.json` updates every hour

**Gotchas:**
- GitHub cron can start several minutes late, which is fine here.
- GitHub disables scheduled workflows on public repos after 60 days of no repo activity (Phase 5 replaces this anyway).
- Community Cloud apps sleep when unused, so the first load after sleeping is slow.

---

## Phase 5 — Airflow + Great Expectations

**Goal:** Airflow replaces the GitHub cron and adds GX validation, alerting and catch-up.

### 5a. Build and test locally first

- [ ] `orchestration/Dockerfile` + `docker-compose.yaml` (LocalExecutor, no Celery/Redis; see 04)
- [ ] Mount `~/.aws` read-only into the containers for local AWS access (local only)
- [ ] `quality/bronze_suite.py`: `bronze_critical` + `bronze_warning` suites (03), with thresholds from `data-profile.md`
- [ ] Test GX on one real hour **and** on a deliberately broken copy (nulls, a negative value, missing rows); confirm the failures
- [ ] `dags/carpark_transform.py` per 04 §2: `compute_window`, `check_bronze_partitions`, `validate_bronze_gx`, `dbt_build_silver`, `dbt_build_gold`, `export_serving`, plus `utils/alerts.py` (`notify_sns`)
- [ ] Airflow Variables: `lake_bucket`, `athena_results_bucket`, `sns_topic_arn` (via `AIRFLOW_VAR_*` env vars)
- [ ] Trigger a run locally → all tasks green

### 5b. Deploy to EC2

- [ ] `infra/network.tf`: VPC, public subnet, IGW, route table + association, security group (egress only)
- [ ] `infra/ec2.tf`: `t3.medium`, instance profile `airflow-ec2-role`, `metadata_options { http_tokens = "required", http_put_response_hop_limit = 2 }`, user data (install Docker + compose plugin, 2 GB swap, systemd unit)
- [ ] Connect with SSM Session Manager → `git clone` the repo → `docker compose up -d`
- [ ] Port-forward 8080 over SSM → log into the Airflow UI → unpause the DAG
- [ ] Set `start_date` to the next hour; once Airflow runs are green, **delete `dbt_hourly.yml` and `github-dbt-role`**

**DoD:**
- [ ] 24 consecutive green hourly runs
- [ ] **Failure test:** set a critical expectation impossible (e.g. row count ≥ 10M) → the task fails → SNS email arrives → revert → clear the task → it goes green
- [ ] **Catch-up test:** stop the instance for 3 hours → start → 3 runs execute in order and serving catches up

**Gotchas:**
- Containers can't reach instance-profile credentials unless the IMDS hop limit is 2.
- Airflow 3 changes from the course notes: imports, `catchup` default, cron data intervals (04 §2).
- If the instance runs out of memory, check the swap file and that Celery services were removed.

---

## Phase 6 — CI/CD + Hardening

**Goal:** every change is tested before merge and deployed automatically.

- [ ] `github-deploy-role` (OIDC)
- [ ] `.github/workflows/ci.yml` (on PR):
  - [ ] `ruff` + `pytest` (ingestion, app)
  - [ ] `terraform fmt -check`, `validate`, `plan`
  - [ ] `dbt build --target dev`
  - [ ] Secret scan (gitleaks)
- [ ] `.github/workflows/deploy.yml` (on push to `main`):
  - [ ] Build the Lambda zip → `terraform apply` (behind a GitHub environment with required approval)
  - [ ] `aws ssm send-command` to `git pull` the DAGs on the Airflow host
- [ ] Security checklist:
  - [ ] S3 Block Public Access on at **account level**
  - [ ] No inbound security group rules
  - [ ] IAM Access Analyzer shows no unintended external access
  - [ ] The Streamlit key is scoped to `serving/*`, with a 90-day rotation reminder

**DoD:** a PR with a deliberately failing dbt test is blocked by CI, and a merged change deploys without manual steps beyond the approval.

---

## Phase 7 — Backtest, README, Demo

**Goal:** a reviewer understands the project in 2 minutes and can verify it works.

- [ ] `mart_baseline_backtest`: for the last 7 days, compare the baseline (built from data *before* that week) against the naive "lots 30 min earlier" forecast; report MAE overall and per agency
- [ ] README:
  - [ ] Problem + the "pipeline creates the dataset" framing
  - [ ] Architecture diagram
  - [ ] Decisions table (link to 01)
  - [ ] Data quality approach
  - [ ] Cost table
  - [ ] Backtest result
  - [ ] Screenshots: Airflow graph, dbt lineage, GX Data Docs, app
  - [ ] "What I'd do next"
- [ ] A 2-minute demo GIF or video of the app
- [ ] `docs/data-log.md` complete (every known ingestion gap explained)

**DoD:** someone unfamiliar with the project can explain from the README what it does, why it's built this way, and whether the baseline beats the naive forecast.

---

## Stretch Goals (after week 8 of data)

| Idea | Why It's Interesting |
|---|---|
| **Nowcast blend:** adjust the baseline by today's current deviation from the median | Usually beats a pure baseline at short horizons, and is cheap to add in the app |
| **ML model** (gradient boosting with day/bucket/carpark features, maybe weather from data.gov.sg) | Compare against the baseline in the same backtest |
| **Semantic layer** (dbt metric definitions) | Shows the C4 semantic-layer concept on real data |
| **Bronze compaction + storage tiering** | Revisit the deferred tiering decision once bronze is large |
| **Streaming write-up** | Document how the design would change if LTA offered a push feed (Kinesis → Firehose → S3) |

---

## Final Portfolio Checklist

- [ ] Ingestion has run continuously for ≥ 4 weeks, with gaps documented
- [ ] All infrastructure is reproducible from `terraform apply`
- [ ] All dbt tests and GX suites pass on scheduled runs
- [ ] Alarms and failure paths have been tested *and* the tests are described in the README
- [ ] The app link works and shows the history depth + freshness
- [ ] The backtest shows how the baseline performs vs. the naive forecast
- [ ] Monthly cost is documented and within target
