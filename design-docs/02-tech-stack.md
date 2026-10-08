# SG Carpark Availability Predictor — Tech Stack & Project Structure

## Technology Stack

Every technology below maps to a concept from the DeepLearning.AI Data Engineering courses (C1 = Intro to DE, C2 = Ingestion & Pipelines, C3 = Storage & Queries, C4 = Modelling & Transformation).

| Layer | Technology | How It's Used | Course |
|---|---|---|---|
| Source | LTA DataMall `CarParkAvailabilityv2` | REST API returning available lots for HDB/LTA/URA carparks | C2 |
| Source (serving) | OneMap Search API | Converts the user's destination into latitude/longitude | C2 |
| Scheduling (ingestion) | Amazon EventBridge Scheduler | Fires the ingestion Lambda every 5 minutes, with a retry policy and a dead-letter queue | C1, C2 |
| Ingestion compute | AWS Lambda (Python 3.12) | Paginates the API, splits good and bad records, writes Parquet to S3 | C1, C2 |
| Secrets | AWS Systems Manager Parameter Store | Stores the LTA API key as a `SecureString` | C1 (security) |
| Storage | Amazon S3 | Data lake with `bronze/`, `silver/`, `gold/`, `serving/` and `quarantine/` zones | C1, C3 |
| Table format | Apache Iceberg | Silver and gold tables, giving ACID merges, hidden partitioning and snapshots | C3 |
| Catalog | AWS Glue Data Catalog | Metastore for the `bronze`, `silver` and `gold` databases; bronze uses partition projection | C3 |
| Query engine | Amazon Athena (engine v3) | Serverless SQL that dbt, GX helpers and the export step run against | C3 |
| Transformation | dbt Core + `dbt-athena-community` | ELT models, SCD2 snapshot, seeds, tests, docs | C4 |
| Data quality | Great Expectations (GX Core 1.x) + dbt tests | GX checks each hour of bronze; dbt tests check modelled tables | C2 |
| Orchestration | Apache Airflow 3 (Docker Compose on EC2, LocalExecutor) | Hourly DAG: check → validate → dbt build → export → alert | C2 |
| Orchestration (MVP) | GitHub Actions scheduled workflow | Runs `dbt build` + export hourly until Airflow is in place | C1 (DataOps) |
| Networking | Amazon VPC (1 public subnet, IGW, security group with no inbound rules) | Hosts the Airflow EC2 instance; reached through SSM port forwarding | C2 |
| Remote access | AWS Systems Manager Session Manager | Opens the Airflow UI without SSH keys or open ports | C1 (security) |
| Monitoring | Amazon CloudWatch (logs, metrics, alarms) | Lambda errors, `RecordsIngested` metric, DLQ depth | C1, C2 |
| Alerting | Amazon SNS (email) | Single alert topic for alarms and Airflow failure callbacks | C1 |
| Serving | Streamlit (Community Cloud) | Reads the serving extract from S3, ranks carparks for a destination | C1 |
| Serving libs | pandas, NumPy, pydeck | Haversine distance, joins, map rendering | C4 |
| IaC | Terraform ≥ 1.11 (AWS provider ~> 6.0) | Provisions all AWS resources; state in an S3 backend with native S3 locking (`use_lockfile`, no DynamoDB table) | C2 |
| CI/CD | GitHub Actions + OIDC to AWS | `terraform plan`, `dbt build` against dev, Lambda deploy | C1 (DataOps) |
| Cost control | AWS Budgets + Athena workgroup scan limit | Alerts on spend; caps bytes scanned per query | C1 (FinOps) |

### Deliberately Not Used

| Technology | Why Not |
|---|---|
| Kinesis Data Streams / Amazon MSK | The source is pulled on a fixed cadence and no producers push events, so streaming adds cost for no forecast gain |
| Spark / EMR / Glue ETL jobs | Daily volume (~hundreds of thousands of rows) fits Athena easily |
| Amazon Redshift | Athena's pay-per-query pricing on S3 is far cheaper at this volume |
| Amazon MWAA | Managed Airflow is expensive for a single portfolio DAG |
| Glue Crawler (for bronze) | Partitions are predictable, and partition projection is instant and free; a crawler would lag the 5-minute writes |
| Lambda inside a VPC | Lambda only calls a public API and S3; putting it in a VPC would need a NAT gateway (paid hourly) |

---

## Dependencies

### Ingestion Lambda (`ingestion/requirements.txt`)

```text
requests>=2.32
```

pandas and pyarrow come from the AWS-managed **AWS SDK for pandas** layer (`AWSSDKPandas-Python312`, matching your runtime and architecture), which also provides `awswrangler`.

### dbt (`dbt/requirements.txt`, installed in its own virtualenv inside the Airflow image)

```text
dbt-core>=1.9
dbt-athena-community>=1.9
```

`dbt/packages.yml`:

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.3.0", "<2.0.0"]
```

### Airflow image (`orchestration/requirements.txt`)

```text
apache-airflow-providers-amazon
great_expectations>=1.0,<2.0
awswrangler>=3.9
```

### Streamlit app (`app/requirements.txt`)

```text
streamlit
pandas
pyarrow
numpy
boto3
requests
pydeck
```

> **⚠️ Pin exact versions** once everything works (`pip freeze > requirements.lock`). The ranges above are starting points; check each package's latest release before you begin.

---

## Project Folder Structure

```
sg-carpark-pipeline/
├── README.md                     ← Architecture diagram, decisions, demo link
├── .gitignore                    ← *.tfvars, .env, secrets.toml, target/, .terraform/
├── docs/                         ← These design documents (01–05)
│
├── infra/                        ← Terraform
│   ├── bootstrap/
│   │   └── main.tf               ← One-off: state bucket (local state)
│   ├── versions.tf               ← terraform {} settings + S3 backend
│   ├── providers.tf              ← provider "aws" (region, default tags)
│   ├── variables.tf
│   ├── outputs.tf                ← bucket names, role ARNs, SNS topic ARN
│   ├── terraform.tfvars          ← (gitignored) account-specific values
│   ├── s3.tf                     ← lake + Athena results buckets, lifecycle, encryption
│   ├── ssm.tf                    ← LTA key parameter (value set manually)
│   ├── iam.tf                    ← one role per component (see 04 IAM matrix)
│   ├── lambda.tf                 ← ingestion function, async invoke config
│   ├── scheduler.tf              ← EventBridge Scheduler + DLQ
│   ├── glue.tf                   ← bronze/silver/gold databases, bronze table
│   ├── athena.tf                 ← workgroup with scan cutoff
│   ├── network.tf                ← VPC, public subnet, IGW, route table, SG
│   ├── ec2.tf                    ← Airflow host (instance profile, IMDSv2 hop limit 2)
│   ├── monitoring.tf             ← SNS topic, CloudWatch alarms, budget
│   └── github_oidc.tf            ← OIDC provider + deploy role (+ MVP-only dbt role)
│
├── ingestion/
│   ├── lambda_function.py        ← Poller (see 04 for logic)
│   ├── requirements.txt
│   ├── build.sh                  ← Zips code + requests into dist/ingest.zip
│   └── tests/
│       └── test_lambda_function.py
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml              ← Reads all settings from env vars
│   ├── packages.yml
│   ├── macros/
│   │   └── generate_schema_name.sql  ← Use custom schema names as-is (silver/gold)
│   ├── seeds/
│   │   └── sg_public_holidays.csv
│   ├── snapshots/
│   │   └── snap_carpark.sql      ← SCD2 source for dim_carpark
│   ├── models/
│   │   ├── sources.yml           ← bronze.carpark_availability_raw (+ freshness)
│   │   ├── silver/
│   │   │   ├── stg_carpark_availability.sql
│   │   │   └── _silver.yml       ← tests + docs
│   │   └── gold/
│   │       ├── dims/  dim_carpark.sql, dim_date.sql, dim_time_bucket.sql
│   │       ├── facts/ fct_carpark_availability.sql
│   │       ├── marts/ mart_availability_baseline.sql, mart_hourly_availability.sql,
│   │       │          mart_carpark_locations.sql, mart_baseline_backtest.sql
│   │       └── _gold.yml
│   └── tests/                    ← Singular SQL tests
│
├── quality/
│   └── bronze_suite.py           ← GX expectation suite + checkpoint builder
│
├── orchestration/
│   ├── Dockerfile                ← FROM apache/airflow:3.x + dbt venv + GX
│   ├── docker-compose.yaml       ← postgres, scheduler, api-server, dag-processor
│   ├── .env                      ← (gitignored) AIRFLOW_UID, etc.
│   └── dags/
│       ├── carpark_transform.py
│       └── utils/ (window.py, export.py, alerts.py)
│
├── app/
│   ├── streamlit_app.py
│   ├── geo.py                    ← OneMap client + vectorised haversine
│   ├── data.py                   ← Cached loaders for serving extract
│   ├── requirements.txt
│   └── .streamlit/secrets.toml   ← (gitignored) read-only AWS keys
│
└── .github/workflows/
    ├── ci.yml                    ← PR: lint, unit tests, terraform plan, dbt build (dev)
    ├── deploy.yml                ← main: terraform apply, Lambda deploy
    └── dbt_hourly.yml            ← MVP-only scheduled dbt build + export
```

---

## How Everything Connects (Data Flow)

```
LTA DataMall API
      │  GET ?$skip=0,500,1000…   (every 5 min)
      ▼
EventBridge Scheduler ──► Lambda (ingest) ──► S3 bronze/   (raw Parquet, dt/hour UTC)
                              │                └─► S3 quarantine/  (bad records, JSON)
                              └─► CloudWatch metric RecordsIngested
      ▼
Glue Data Catalog (bronze table, partition projection) ◄── Athena
      ▼
Airflow DAG carpark_transform  (hourly at :15, Asia/Singapore)
   1. check_bronze_partitions   – enough polls landed?
   2. validate_bronze_gx        – GX checkpoint on the hour
   3. dbt_build_silver          – stg model + snapshot + tests   → S3 silver/ (Iceberg)
   4. dbt_build_gold            – dims, fact, marts + tests       → S3 gold/   (Iceberg)
   5. export_serving            – marts → S3 serving/*.parquet + manifest
   (on failure → SNS email)
      ▼
Streamlit app ──► reads serving/ (cached 1 h) ──► OneMap geocode ──► rank ──► user
```

### Concrete Example: One Poll's Journey

1. **10:05:12 SGT**: EventBridge Scheduler invokes the ingestion Lambda.
2. **Lambda** reads the API key from SSM (cached while the container stays warm) and requests pages at `$skip=0, 500, 1000…` until a page returns fewer than 500 records.
3. **Lambda** validates each record. Good rows become a DataFrame with `polled_at_utc = 02:05:12` and `poll_id`. Bad rows are written to `quarantine/`.
4. **Lambda** writes `s3://carpark-lake-<acct>/bronze/carpark_availability/dt=2026-10-12/hour=02/poll_20261012T020512Z.parquet` and publishes `RecordsIngested`.
5. **11:15 SGT**: the Airflow run for the 10:00–11:00 SGT window (02:00–03:00 UTC) checks that ~12 poll files exist and runs the GX checkpoint on that hour.
6. **dbt** merges the hour into `silver.stg_carpark_availability`. The poll becomes slot `10:05` SGT, bucket 20 (10:00–10:29), day type `weekday`. dbt then updates the `snap_carpark` snapshot, merges `gold.fct_carpark_availability` and rebuilds the marts.
7. **export_serving** writes `serving/baseline.parquet`, `serving/carparks.parquet` and `serving/manifest.json`.
8. A user searching "Plaza Singapura" for a 10:15 arrival sees the median lots for bucket 20 on weekdays, which now includes this poll.

---

## Coding Conventions

| Concern | Convention |
|---|---|
| Python | 3.12; type hints; `logging` (not `print`) in Lambda and DAG code; unit tests with `pytest` |
| Time | Store UTC in bronze (`polled_at_utc`); convert to SGT **once** in `stg_carpark_availability`; suffix columns `_utc` / `_sgt` |
| Naming | snake_case everywhere; dbt prefixes `stg_`, `snap_`, `dim_`, `fct_`, `mart_` |
| Keys | Natural key = `carpark_id` + `agency`; surrogate keys via `dbt_utils.generate_surrogate_key` (MD5) |
| Idempotency | Every scheduled job processes an explicit window and writes with `merge` or full overwrite, so re-runs are safe |
| Secrets | Never in code or Git: SSM (Lambda), instance profile (Airflow), `secrets.toml` (Streamlit), OIDC (GitHub) |
| Terraform | One file per concern; no credentials in `provider` blocks (use `AWS_PROFILE`); `terraform fmt` + `validate` in CI |
| dbt | Every model has a description and at least one test in its `.yml`; marts have column docs |
| Airflow | No top-level code in DAG files; config via Airflow Variables; XCom for small metadata only (data stays in S3) |
| Git | `main` is protected; feature branches + PRs so CI runs before merge |

---

## Course Concepts Mapped to Project Features

| Course Concept | Where It Appears |
|---|---|
| Stakeholders + requirements framework (C1) | Doc 01: EPAS table, BR/SR/FR/NFR requirements |
| Regions & AZs (C1) | `ap-southeast-1`: closest to users and the data source |
| Shared responsibility + IAM least privilege (C1) | One role per component; IAM matrix in doc 04 |
| Undercurrents: DataOps, orchestration, security (C1) | CI/CD, CloudWatch/SNS, Airflow, SSM |
| Architecture principles: reversible decisions, loose coupling, FinOps (C1) | Ingestion decoupled from orchestration; open Iceberg format; budget + scan limits |
| Batch vs streaming ingestion (C2) | Fixed-interval batch chosen, streaming documented as rejected (D1) |
| REST API ingestion, sessions, pagination, error handling (C2) | Lambda poller with `requests.Session`, `$skip` loop, retries |
| ETL vs ELT (C2) | ELT: raw → S3, transform with dbt on Athena |
| Networking: VPC, subnets, IGW, route tables, security groups (C2) | `network.tf` for the Airflow host; Lambda deliberately outside a VPC |
| Terraform settings/providers/resources/variables/outputs/backend (C2) | `infra/` |
| Data quality monitoring + Great Expectations (C2) | `quality/bronze_suite.py`, CloudWatch metrics |
| Airflow DAGs, operators, XCom, Variables, TaskFlow, catchup (C2) | `carpark_transform` DAG (doc 04) |
| Row vs columnar storage (C3) | Parquet + Iceberg; Athena reads only the columns it needs |
| Lake zones, partitioning, data catalog (C3) | bronze/silver/gold; `dt`/`hour` + Iceberg `day()` partitions; Glue Catalog |
| Lakehouse + medallion architecture (C3) | Iceberg tables on S3 with ACID merges |
| Query performance (C3) | Partition pruning, pre-aggregated marts, serving extract (caching) |
| Storage tiers (C3) | Tiering evaluated and deliberately deferred (doc 03) |
| Normalisation → star schema, grain, surrogate keys (C4) | `stg` → dims + fact; grain table in doc 03 |
| Kimball vs Inmon vs Data Vault (C4) | Decision D6 |
| Materialized views (C4) | Gold marts are tables rebuilt hourly (materialized results) |
| Pandas vs Spark (C4) | pandas for small, single-node work; Spark rejected |
| Semantic layer (C4) | Stretch goal: dbt metric definitions for "expected available lots" |

---

## Config Templates

### `infra/terraform.tfvars`

```hcl
region           = "ap-southeast-1"
project          = "carpark"
alert_email      = "you@example.com"
github_repo      = "your-github-username/sg-carpark-pipeline"
monthly_budget   = 50
airflow_instance_type = "t3.medium"
```

### `dbt/profiles.yml`

```yaml
carpark:
  target: "{{ env_var('DBT_TARGET', 'prod') }}"
  outputs:
    prod:
      type: athena
      region_name: ap-southeast-1
      database: awsdatacatalog
      schema: silver
      work_group: carpark
      s3_staging_dir: "s3://{{ env_var('ATHENA_RESULTS_BUCKET') }}/dbt/"
      s3_data_dir: "s3://{{ env_var('LAKE_BUCKET') }}/"
      s3_data_naming: schema_table_unique
      threads: 4
    dev:
      type: athena
      region_name: ap-southeast-1
      database: awsdatacatalog
      schema: dev_silver
      work_group: carpark
      s3_staging_dir: "s3://{{ env_var('ATHENA_RESULTS_BUCKET') }}/dbt-dev/"
      s3_data_dir: "s3://{{ env_var('LAKE_BUCKET') }}/dev/"
      s3_data_naming: schema_table_unique
      threads: 4
```

### `app/.streamlit/secrets.toml`

```toml
[aws]
access_key_id     = "AKIA..."      # IAM user: s3:GetObject on serving/* only
secret_access_key = "..."
region            = "ap-southeast-1"
lake_bucket       = "carpark-lake-<account_id>"

[onemap]
token = "..."                      # optional, for higher rate limits
```

### SSM parameter (created once by hand; Terraform creates the parameter with a placeholder)

```bash
aws ssm put-parameter --name /carpark/lta_account_key \
  --type SecureString --value "<your-lta-key>" --overwrite
```

> **Important:** `terraform.tfvars`, `.env`, `secrets.toml` and any `*.tfstate` file are gitignored and must never be committed.
