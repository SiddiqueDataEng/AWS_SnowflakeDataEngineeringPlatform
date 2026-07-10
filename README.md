# 🇵🇰 Pakistani AWS + Snowflake Data Engineering Platform

A locally-simulated, production-aligned data engineering platform built on **AWS** and **Snowflake** — with Pakistani contextual data (CNIC IDs, PKR pricing, local couriers, GST rates, Pakistani cities and names).

> Full production deployment guide → **[PRODUCTION_GUIDE.md](PRODUCTION_GUIDE.md)**

---

## Quick Start

```powershell
# Install dependencies
python -m pip install -r requirements.txt

# Run full pipeline (all 11 steps)
python run_pipeline.py

# Interactive menu (PowerShell)
.\run.ps1

# Interactive menu (CMD)
run.bat

# Launch BI Dashboard
streamlit run superset_dashboard.py
```

---

## Project Structure

```
aws_snowflake_data_eng/
│
├── 01_setup/                        Snowflake DDL — database, schemas, tables, clustering
│   ├── initial_setup.sql            Create PK_ECOMMERCE_DB with CUSTOMER, ORDERS, LINEITEM
│   └── consumption_tracking.sql     Warehouse credit + query cost monitoring
│
├── 02_data_generator/               Pakistani contextual data generator
│   └── pk_data_generator.py         500 customers · 2000 orders · ~5000 lineitems
│                                    Names, CNICs, cities, PKR prices, local couriers
│
├── 03_data_loading/                 Data ingestion layer
│   ├── setup_db.py                  Local COPY INTO simulation (SQLite)
│   ├── load_data.sql                S3 → Snowflake: storage integration, stages, Snowpipe
│   └── file_formats.sql             CSV / JSON / Parquet file format definitions
│
├── 04_streams_cdc/                  Change Data Capture
│   └── streams_cdc.py               Standard delta streams, append-only, transactional MERGE
│
├── 05_tasks_scheduling/             Snowflake Task orchestration
│   └── tasks.py                     Daily sales aggregation, dependent task chain, task history
│
├── 06_udf/                          User Defined Functions
│   └── udf_simulation.py            PKR→USD conversion, GST calculator, tabular UDTF, RBAC
│
├── 07_external_functions/           AWS Lambda + API Gateway
│   └── lambda_currency.py           PKR/USD/AED/SAR exchange rates via Snowflake external func
│
├── 08_aws_python/                   AWS Python integrations
│   ├── snowflake_connector.py       Connector, pandas, Glue, PySpark-style transform
│   └── airflow_dags/
│       └── pk_sales_dag.py          Airflow DAG: COPY INTO >> Glue PySpark aggregation
│
├── 09_kafka_streaming/              Real-time streaming
│   ├── pk_kafka_producer.py         Pakistani sales events (Easypaisa, JazzCash, TCS, Leopards)
│   └── pk_kafka_consumer.py         Kafka → Snowflake sink (buffer-based micro-batching)
│
├── 10_governance_security/          Data Governance
│   └── governance.py                CNIC/phone masking, province row policies, time-travel
│
├── 11_snowpark/                     Snowpark Python API
│   └── snowpark_simulation.py       DataFrame ops, stored procs, UDFs, ML sales forecast
│
├── terraform/                       Infrastructure as Code (AWS + Snowflake)
│   ├── main.tf                      Root config — wires all modules
│   ├── variables.tf                 All input variables with defaults
│   ├── outputs.tf                   Key resource identifiers post-apply
│   ├── deploy.ps1                   PowerShell deploy script (plan/apply/destroy)
│   ├── deploy.bat                   CMD deploy script
│   ├── environments/
│   │   ├── dev/terraform.tfvars     Dev overrides (smaller instances)
│   │   └── prod/terraform.tfvars    Production sizes + alerting
│   └── modules/
│       ├── s3/                      Data lake buckets + KMS encryption + lifecycle
│       ├── iam/                     IAM roles (Snowflake, Glue, MWAA, Lambda) + SGs
│       ├── glue/                    PySpark ETL job + script upload
│       ├── msk/                     Managed Kafka (3-broker, TLS, 7-day retention)
│       ├── mwaa/                    Managed Airflow 2.8 + DAG sync
│       ├── lambda/                  Currency Lambda + HTTP API Gateway
│       ├── snowflake/               Full Snowflake env: DB, warehouses, stages, masking
│       └── monitoring/              CloudWatch alarms, SNS (email + +92 SMS), budget
│
├── local_db/
│   └── pk_ecommerce.db              SQLite — local Snowflake simulation (22 tables)
│
├── local_s3/                        Local S3 simulation
│   ├── pk_ecommerce_dev/            CSV + JSON data files
│   └── streaming/                   Kafka topic JSONL files
│
├── logs/                            Per-step execution logs
│
├── run.ps1                          PowerShell launcher — menu or direct: .\run.ps1 1
├── run.bat                          CMD launcher — menu or direct: run.bat 1
├── run_pipeline.py                  Run all 11 steps in sequence
├── superset_dashboard.py            BI Dashboard (Streamlit + Plotly)
├── check_db.py                      Print all table row counts
├── requirements.txt                 Python dependencies
├── test_all_steps.ps1               Smoke test — runs all steps, reports pass/fail
└── PRODUCTION_GUIDE.md              ← Full production deployment guide
```

---

## Pipeline Steps

| # | Module | What runs |
|---|--------|-----------|
| 1 | `02_data_generator` | Generate 500 customers, 2000 orders, ~5000 lineitems (Pakistani names, CNICs, PKR, GST) |
| 2 | `03_data_loading` | Create SQLite DB, COPY INTO all tables from CSV |
| 3 | `04_streams_cdc` | Telecom subscriber CDC — delta streams, transactional MERGE to prepaid/postpaid tables |
| 4 | `05_tasks_scheduling` | Daily PKR sales aggregation task chain with task history log |
| 5 | `06_udf` | Scalar UDFs (PKR→USD, GST), tabular UDTF, UDF RBAC |
| 6 | `07_external_functions` | Lambda PKR/USD/AED/SAR currency conversion (batch + single) |
| 7 | `08_aws_python` | Python connector, pandas query, Glue job, parameterized Glue, PySpark join+aggregate |
| 8 | `08_aws_python/airflow_dags` | Airflow DAG: COPY INTO task >> Glue PySpark transform |
| 9 | `09_kafka_streaming` | Produce 50 Pakistani sales events → consume → SALES_DATA table |
| 10 | `10_governance_security` | Column masking (CNIC, phone), province row policies, time-travel snapshot/restore |
| 11 | `11_snowpark` | DataFrame ops, stored proc, Snowpark UDFs, LinearRegression sales forecast (R²=0.88) |

Run a specific step:

```powershell
.\run.ps1 -Component 3      # PowerShell
run.bat 3                   # CMD
```

---

## BI Dashboard

```powershell
streamlit run superset_dashboard.py
# or
.\run.ps1 -Component dashboard
```

Opens at **http://localhost:8501** with 11 views:

| View | Content |
|------|---------|
| Executive Summary | 6 KPI cards, revenue by province, order status, top cities, courier share |
| Sales by Province | Bar + pie + treemap Province → City drilldown |
| Revenue Trend | Daily/Weekly/Monthly area chart, order count bar |
| Orders Analysis | Priority breakdown, status dual-axis, Pakistan GST distribution |
| Top Customers | Lifetime value ranking, gender split, sortable table |
| Supplier Performance | Revenue, scatter bubble, avg discount % |
| Shipping & Courier | Shipmode bars, delivery days, heatmap |
| Product Categories | Category revenue pie + bar, top products per category |
| Streaming Sales | Live Kafka events — city revenue, payment method, categories |
| ML Forecast | Actual vs predicted revenue per province (R²=0.88) |
| SQL Explorer | Free-form SQL + auto-chart on results |

---

## Terraform Deploy

> Requires: Terraform ≥ 1.6, AWS CLI, Snowflake account

```powershell
# Fill in terraform/environments/dev/terraform.tfvars first
# then:
.\terraform\deploy.ps1 plan  dev
.\terraform\deploy.ps1 apply dev

# Production
.\terraform\deploy.ps1 plan  prod
.\terraform\deploy.ps1 apply prod
```

What gets provisioned:

| Module | AWS/Snowflake resources |
|--------|------------------------|
| `s3` | 3 buckets (data-lake, airflow, glue) + KMS + lifecycle (30d→IA→90d→Glacier→7yr) |
| `iam` | Snowflake, Glue, MWAA, Lambda IAM roles + MSK/MWAA security groups |
| `glue` | PySpark ETL job (`pk_sales_agg`) with Snowflake JDBC jars |
| `msk` | 3-broker Kafka cluster, TLS, replication factor 3 |
| `mwaa` | Managed Airflow 2.8, DAG auto-sync from S3 |
| `lambda` | PKR currency Lambda + HTTP API Gateway (Snowflake External Function) |
| `snowflake` | DB + 3 schemas, 2 warehouses, storage integration, file formats, stages, roles, masking policies |
| `monitoring` | CloudWatch alarms, SNS email + Pakistani SMS (+92xxx), $1,500/mo budget alert |

---

## Pakistani Data Context

| Entity | Details |
|--------|---------|
| Customers | Pakistani names (Ahmed Khan, Fatima Malik …), CNIC format `DDDDD-DDDDDDD-D`, mobile `03XX-XXXXXXX` |
| Cities | Karachi, Lahore, Islamabad, Rawalpindi, Peshawar, Quetta, Multan, Faisalabad, Hyderabad, Sialkot … |
| Products | Mobile phones, electronics, clothing, food, agriculture, textiles — all PKR priced |
| Tax | Pakistan GST rates: 0%, 5%, 17% |
| Couriers | TCS, Leopards Courier, M&P Express, Pakistan Post, Trax, Swyft |
| Payment | Easypaisa, JazzCash, HBL/MCB/UBL bank transfer, COD |
| Currency | PKR (Pakistani Rupee) — conversion via Lambda external function |
| Suppliers | 10 regional suppliers across Karachi, Lahore, Islamabad, Multan, Peshawar, Quetta, Faisalabad, Sialkot, Hyderabad, Gujranwala |

---

## Requirements

```
faker>=19.0.0
pandas>=2.0.0
scikit-learn>=1.3.0
streamlit          # for BI Dashboard
plotly             # for BI Dashboard
```

Install: `pip install -r requirements.txt`

---

## Production Readiness

The local simulation is **design-complete** for production. Seven mechanical substitutions needed:

| Local (POC) | Production |
|-------------|------------|
| `sqlite3` | Snowflake `snowflake-connector-python` + key-pair auth |
| `local_s3/` CSV files | AWS S3 bucket (`pk-data-lake-prod`) |
| `LocalQueueProducer` (JSONL) | Amazon MSK (Kafka) |
| `run_dag_standalone()` | Amazon MWAA (Managed Airflow) |
| Mock exchange rates | AWS Lambda + real SBP/open.er-api |
| In-memory snapshots | Snowflake native `AT/BEFORE` time-travel |
| SQLite triggers | Snowflake Streams + Tasks |

Estimated migration effort: **~17 hours**.

→ See **[PRODUCTION_GUIDE.md](PRODUCTION_GUIDE.md)** for step-by-step instructions, all CLI commands, cost estimates (PKR + USD), security checklist, and 4-week go-live plan.

---

*AWS ap-south-1 (Mumbai) · Snowflake Enterprise · Python 3.12 · Terraform 1.6+*
