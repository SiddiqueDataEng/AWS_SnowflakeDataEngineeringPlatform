# 🇵🇰 Pakistani Ecommerce — AWS + Snowflake Production Guide

> **POC → Production deployment guide** for the Pakistani retail data engineering platform.
> Covers: architecture, gap analysis, tool stack, step-by-step production deployment, and operations.

---

## Table of Contents

1. [Is This POC Production-Ready?](#1-is-this-poc-production-ready)
2. [Architecture Overview](#2-architecture-overview)
3. [POC vs Production Gap Analysis](#3-poc-vs-production-gap-analysis)
4. [Full Tool Stack](#4-full-tool-stack)
5. [AWS Setup — Step by Step](#5-aws-setup--step-by-step)
6. [Snowflake Setup — Step by Step](#6-snowflake-setup--step-by-step)
7. [Connecting AWS to Snowflake](#7-connecting-aws-to-snowflake)
8. [Airflow Deployment (MWAA)](#8-airflow-deployment-mwaa)
9. [Kafka Streaming (MSK)](#9-kafka-streaming-msk)
10. [Data Governance in Production](#10-data-governance-in-production)
11. [Monitoring & Alerting](#11-monitoring--alerting)
12. [Cost Estimate (PKR / USD)](#12-cost-estimate-pkr--usd)
13. [Security Hardening Checklist](#13-security-hardening-checklist)
14. [Disaster Recovery](#14-disaster-recovery)
15. [Go-Live Checklist](#15-go-live-checklist)

---

## 1. Is This POC Production-Ready?

**Short answer: The design is production-grade. The implementation needs 7 replacements before go-live.**

### What IS production-ready right now

| Component | POC Status | Notes |
|-----------|-----------|-------|
| Snowflake DDL (`initial_setup.sql`) | ✅ Production SQL | Clustering keys, time-travel, FK constraints — all correct Snowflake syntax |
| Data model | ✅ Solid | CUSTOMER, SUPPLIER, PART, ORDERS, LINEITEM with correct Pakistani domain |
| Storage Integration SQL | ✅ Ready | `load_data.sql` just needs real ARN + bucket values filled in |
| Snowflake RBAC | ✅ Production design | Masking policies, row-level policies, role hierarchy all match best practices |
| Snowpipe auto-ingest | ✅ Correct SQL | `pk_orders_pipe` and `pk_lineitem_pipe` ready for real S3 + SNS |
| CDC Stream + Task SQL | ✅ Correct design | Delta streams + MERGE tasks map 1:1 to Snowflake Tasks API |
| Airflow DAG structure | ✅ Correct pattern | `copy_task >> glue_task` dependency chain is production-standard |
| Pakistani data context | ✅ Realistic | CNIC, Pakistani cities, GST rates, local couriers, PKR pricing |

### What needs to be replaced for production

| # | POC component | Replace with | Effort |
|---|--------------|--------------|--------|
| 1 | `sqlite3` database | Real Snowflake account | 2h |
| 2 | `LocalSnowflakeConnection` class | `snowflake.connector.connect(...)` | 1h |
| 3 | Local CSV files in `local_s3/` | AWS S3 bucket | 2h |
| 4 | `LocalQueueProducer` (JSONL file) | Amazon MSK (Kafka) | 4h |
| 5 | Standalone `run_dag_standalone()` | Amazon MWAA (Airflow) | 4h |
| 6 | `lambda_handler` mock rates | Real Lambda + API Gateway + SBP/open.er-api | 3h |
| 7 | In-memory snapshots for time-travel | Snowflake native time-travel (`AT/BEFORE`) | 1h |

**Total migration effort: ~17 hours** for an experienced data engineer.

---

## 2. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    PAKISTANI ECOMMERCE DATA PLATFORM                         │
│                         AWS + Snowflake — Production                         │
└─────────────────────────────────────────────────────────────────────────────┘

  ┌──────────────────────────────────────────────────────────────────────────┐
  │  DATA SOURCES                                                             │
  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌─────────────┐ │
  │  │ ERP / POS    │  │ Mobile App   │  │ Courier APIs │  │ SBP FX API  │ │
  │  │ (Orders,     │  │ (Kafka       │  │ TCS, Leopards│  │ (Currency   │ │
  │  │  Customers)  │  │  events)     │  │ M&P, Trax    │  │  Rates)     │ │
  │  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └──────┬──────┘ │
  └─────────│─────────────────│─────────────────│──────────────────│────────┘
            │                 │                 │                  │
  ┌─────────▼─────────────────▼─────────────────▼──────────────────▼────────┐
  │  AWS INGESTION LAYER (ap-south-1 / me-south-1)                           │
  │                                                                           │
  │  ┌────────────────┐   ┌──────────────────┐   ┌───────────────────────┐  │
  │  │  Amazon S3     │   │  Amazon MSK      │   │  AWS Lambda           │  │
  │  │  (Data Lake)   │   │  (Kafka Cluster) │   │  + API Gateway        │  │
  │  │                │   │                  │   │  (External Functions) │  │
  │  │  pk-data-raw/  │   │  pk-sales-data   │   │                       │  │
  │  │  pk-data-proc/ │   │  topic           │   │  pkr_currency_convert │  │
  │  │  pk-data-arch/ │   │                  │   │  delivery_status      │  │
  │  └────────┬───────┘   └──────────┬───────┘   └───────────────────────┘  │
  │           │           Kafka SF   │ Connector                             │
  │  SNS      │           ┌──────────┘                                       │
  │  trigger  │           │                                                   │
  └───────────│───────────│──────────────────────────────────────────────────┘
              │           │
  ┌───────────▼───────────▼──────────────────────────────────────────────────┐
  │  SNOWFLAKE DATA WAREHOUSE (AWS-hosted, ap-south-1)                       │
  │                                                                           │
  │  ┌────────────────────────────────────────────────────────────────────┐  │
  │  │  pk_ecommerce_db                                                   │  │
  │  │  ├── pk_ecommerce_raw    ← COPY INTO (Snowpipe auto-ingest)       │  │
  │  │  ├── pk_ecommerce_dev    ← staging, transformations               │  │
  │  │  └── pk_ecommerce_liv    ← production (clustered, time-travel)    │  │
  │  │       ├── CUSTOMER  (500K+ rows, province RBAC)                   │  │
  │  │       ├── ORDERS    (clustered by O_ORDERDATE)                    │  │
  │  │       ├── LINEITEM  (clustered by L_SHIPDATE, GST rates)          │  │
  │  │       ├── SUPPLIER  (10 regional suppliers)                       │  │
  │  │       └── PART      (6 categories, PKR pricing)                   │  │
  │  │                                                                    │  │
  │  │  Streams (CDC) → Tasks (MERGE) → Aggregated tables               │  │
  │  │  Masking policies on CNIC, phone                                  │  │
  │  │  Row-level policies by province                                   │  │
  │  │  Time-travel: 30 days retention                                   │  │
  │  └────────────────────────────────────────────────────────────────────┘  │
  └───────────────────────────────────────┬──────────────────────────────────┘
                                          │
  ┌───────────────────────────────────────▼──────────────────────────────────┐
  │  ORCHESTRATION & COMPUTE (AWS)                                            │
  │                                                                           │
  │  ┌────────────────┐   ┌────────────────┐   ┌───────────────────────┐    │
  │  │  Amazon MWAA   │   │  AWS Glue      │   │  Snowpark (Python)    │    │
  │  │  (Airflow)     │──▶│  (PySpark ETL) │──▶│  ML Forecast          │    │
  │  │                │   │                │   │  Stored Procs         │    │
  │  │  pk_sales_dag  │   │  pk_sales_agg  │   │  UDFs                 │    │
  │  │  @daily 08:00  │   │  Glue job      │   │                       │    │
  │  └────────────────┘   └────────────────┘   └───────────────────────┘    │
  └───────────────────────────────────────┬──────────────────────────────────┘
                                          │
  ┌───────────────────────────────────────▼──────────────────────────────────┐
  │  ANALYTICS & VISUALIZATION                                                │
  │                                                                           │
  │  ┌────────────────┐   ┌────────────────┐   ┌───────────────────────┐    │
  │  │  Apache        │   │  Streamlit     │   │  Snowflake            │    │
  │  │  Superset      │   │  Dashboard     │   │  Worksheets           │    │
  │  │  (BI/Charts)   │   │  (Operational) │   │  (Ad-hoc SQL)         │    │
  │  └────────────────┘   └────────────────┘   └───────────────────────┘    │
  └──────────────────────────────────────────────────────────────────────────┘
```

### Data Flow Patterns

| Pattern | Flow | Latency |
|---------|------|---------|
| Batch load | S3 → Snowpipe → LINEITEM | < 5 min |
| Real-time stream | App → MSK → Kafka Connector → SALES_DATA | < 60 sec |
| CDC pipeline | Source → Stream → Task (MERGE) → Prod table | 1 min |
| ETL orchestration | Airflow → Glue PySpark → Snowflake | Daily 08:00 PKT |
| External function | Snowflake → API Gateway → Lambda → rate | < 2 sec |
| ML forecast | Snowpark → Snowflake ML | On-demand |

---

## 3. POC vs Production Gap Analysis

### Storage

| Feature | POC (local) | Production |
|---------|------------|------------|
| Storage | `local_s3/` directory | S3 bucket `pk-data-lake-prod` (ap-south-1) |
| File format | CSV + JSON | Parquet (columnar, 3–5× compression) |
| Partitioning | None | `year=/month=/day=/` partitioned prefix |
| Versioning | None | S3 versioning enabled |
| Encryption | None | SSE-KMS (AWS KMS key per environment) |
| Lifecycle | None | Raw → 30d → Glacier; Archive → 7yr (SECP compliance) |

### Database / Warehouse

| Feature | POC | Production |
|---------|-----|------------|
| Engine | SQLite | Snowflake (Enterprise or Business Critical) |
| Connector | `sqlite3` | `snowflake-connector-python` + key-pair auth |
| Credentials | Hardcoded | AWS Secrets Manager → Snowflake key-pair |
| Warehouse size | N/A | `X-SMALL` for ETL, `MEDIUM` for BI queries |
| Auto-suspend | N/A | 60 seconds (cost control) |
| Multi-cluster | N/A | Enabled for BI workloads during peak hours |

### Streaming

| Feature | POC | Production |
|---------|-----|------------|
| Broker | `LocalQueueProducer` (JSONL) | Amazon MSK (3-broker, `kafka.m5.large`) |
| Topic config | N/A | Replication factor 3, retention 7 days |
| Schema | JSON (untyped) | Avro with AWS Glue Schema Registry |
| Consumer | File reader | Snowflake Kafka Connector (sink) |
| Ordering | N/A | Partition by `C_CUSTKEY` for ordering guarantee |

### Orchestration

| Feature | POC | Production |
|---------|-----|------------|
| Scheduler | `run_dag_standalone()` | Amazon MWAA 2.x |
| DAG store | Local Python file | S3 bucket `pk-airflow-dags/` |
| Secrets | Hardcoded paths | Airflow Connections + AWS Secrets Manager |
| Retries | 1 × 5 min | 3 × exponential backoff |
| Alerting | None | SNS → email/SMS on task failure |

### Security

| Feature | POC | Production |
|---------|-----|------------|
| Auth | No auth | Snowflake: key-pair + MFA for humans |
| Secrets | Env vars / hardcoded | AWS Secrets Manager (auto-rotation 90d) |
| Network | None | Snowflake Private Link + VPC endpoint for S3 |
| CNIC masking | Python dict | Snowflake Dynamic Data Masking policy |
| Row policy | Python filter | Snowflake Row Access Policy + access management table |
| Audit log | None | Snowflake `ACCESS_HISTORY` + CloudTrail |

---

## 4. Full Tool Stack

### Core Platform

| Tool | Version | Role | Where |
|------|---------|------|-------|
| **Snowflake** | Enterprise | Data Warehouse | AWS ap-south-1 |
| **Amazon S3** | — | Data Lake | ap-south-1 |
| **Amazon MSK** | Kafka 3.5 | Streaming | ap-south-1 |
| **AWS Glue** | 4.0 (Spark 3.3) | ETL / PySpark | ap-south-1 |
| **Amazon MWAA** | Airflow 2.8 | Orchestration | ap-south-1 |
| **AWS Lambda** | Python 3.12 | External Functions | ap-south-1 |
| **API Gateway** | HTTP API | Lambda trigger | ap-south-1 |
| **AWS Secrets Manager** | — | Credential store | ap-south-1 |

### Data & Processing

| Tool | Version | Role |
|------|---------|------|
| **Python** | 3.12 | Glue jobs, Lambda, connectors |
| **PySpark** | 3.3 | Large-scale ETL in Glue |
| **Snowpark** | 1.x | Python DataFrame API inside Snowflake |
| **pandas** | 2.x | Light transforms, local dev |
| **Faker** | 19.x | Pakistani test data generation |
| **scikit-learn** | 1.3 | ML forecasting via Snowpark |

### Kafka Ecosystem

| Tool | Role |
|------|------|
| **Amazon MSK** | Managed Kafka cluster |
| **Snowflake Kafka Connector** | Sink connector (MSK → Snowflake) |
| **Glue Schema Registry** | Avro schema management |
| **kafka-python** | Producer client |

### Visualization

| Tool | Role | URL |
|------|------|-----|
| **Apache Superset** | Main BI dashboard | Port 8088 |
| **Streamlit** | Operational dashboard (this POC) | Port 8501 |
| **Snowflake Worksheets** | Ad-hoc SQL | Snowflake UI |

### IaC & DevOps

| Tool | Role |
|------|------|
| **Terraform** | All AWS resource provisioning |
| **AWS CDK** | Optional: Python-native IaC |
| **GitHub Actions** | CI/CD — lint, test, deploy DAGs |
| **pre-commit** | SQL formatting, Python linting |
| **pytest** | Unit tests for Python jobs |

---

## 5. AWS Setup — Step by Step

### Prerequisites
- AWS account with `AdministratorAccess` (use a dedicated `dataeng-prod` account)
- AWS CLI configured: `aws configure --profile dataeng-prod`
- Terraform ≥ 1.6 installed
- Python 3.12 with `boto3`, `snowflake-connector-python`

---

### Step 1: Create S3 Data Lake

```bash
# Create buckets (replace pk-dataeng-12345 with a unique suffix)
aws s3 mb s3://pk-data-lake-prod-12345      --region ap-south-1
aws s3 mb s3://pk-airflow-dags-12345        --region ap-south-1
aws s3 mb s3://pk-glue-assets-12345         --region ap-south-1

# Enable versioning on raw data bucket
aws s3api put-bucket-versioning \
  --bucket pk-data-lake-prod-12345 \
  --versioning-configuration Status=Enabled

# Enable server-side encryption (SSE-KMS)
aws s3api put-bucket-encryption \
  --bucket pk-data-lake-prod-12345 \
  --server-side-encryption-configuration '{
    "Rules": [{
      "ApplyServerSideEncryptionByDefault": {
        "SSEAlgorithm": "aws:kms"
      }
    }]
  }'

# Create folder structure
aws s3api put-object --bucket pk-data-lake-prod-12345 --key pk_ecommerce_dev/customers/
aws s3api put-object --bucket pk-data-lake-prod-12345 --key pk_ecommerce_dev/orders/
aws s3api put-object --bucket pk-data-lake-prod-12345 --key pk_ecommerce_dev/lineitems/
aws s3api put-object --bucket pk-data-lake-prod-12345 --key pk_ecommerce_dev/suppliers/
aws s3api put-object --bucket pk-data-lake-prod-12345 --key pk_ecommerce_dev/parts/
```

---

### Step 2: IAM Role for Snowflake

```bash
# Create trust policy for Snowflake
cat > snowflake-trust-policy.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {
      "AWS": "arn:aws:iam::SNOWFLAKE_ACCOUNT_ID:root"
    },
    "Action": "sts:AssumeRole",
    "Condition": {
      "StringEquals": {
        "sts:ExternalId": "SNOWFLAKE_EXTERNAL_ID"
      }
    }
  }]
}
EOF

# Create the IAM role
aws iam create-role \
  --role-name pk-snowflake-s3-role \
  --assume-role-policy-document file://snowflake-trust-policy.json

# Attach S3 policy
aws iam put-role-policy \
  --role-name pk-snowflake-s3-role \
  --policy-name SnowflakeS3Access \
  --policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Action": [
        "s3:GetObject", "s3:GetObjectVersion",
        "s3:PutObject", "s3:DeleteObject",
        "s3:ListBucket", "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::pk-data-lake-prod-12345",
        "arn:aws:s3:::pk-data-lake-prod-12345/*"
      ]
    }]
  }'

# Note the ARN — you'll need it in Snowflake
aws iam get-role --role-name pk-snowflake-s3-role --query 'Role.Arn'
```

---

### Step 3: Store Secrets in AWS Secrets Manager

```bash
# Snowflake credentials (use key-pair auth in production)
aws secretsmanager create-secret \
  --name "prod/pk-platform/snowflake" \
  --region ap-south-1 \
  --secret-string '{
    "account":   "YOUR_ACCOUNT.ap-south-1.aws",
    "user":      "svc_dataeng",
    "private_key_path": "/etc/snowflake/rsa_key.p8",
    "warehouse": "COMPUTE_WH",
    "database":  "pk_ecommerce_db",
    "role":      "SYSADMIN"
  }'

# Retrieve in Python
import boto3, json
def get_snowflake_creds():
    client = boto3.client('secretsmanager', region_name='ap-south-1')
    return json.loads(
        client.get_secret_value(SecretId='prod/pk-platform/snowflake')['SecretString']
    )
```

---

### Step 4: AWS Glue Job

```bash
# Upload PySpark script to S3
aws s3 cp aws_snowflake_data_eng/08_aws_python/snowflake_connector.py \
  s3://pk-glue-assets-12345/scripts/pk_sales_transform.py

# Upload Snowflake JDBC jars
aws s3 cp snowflake-jdbc-3.15.0.jar \
  s3://pk-glue-assets-12345/jars/
aws s3 cp spark-snowflake_2.12-2.12.0-spark_3.3.jar \
  s3://pk-glue-assets-12345/jars/

# Create Glue job
aws glue create-job \
  --name "pk_sales_agg" \
  --role "arn:aws:iam::ACCOUNT_ID:role/AWSGlueServiceRole" \
  --command '{"Name":"glueetl","ScriptLocation":"s3://pk-glue-assets-12345/scripts/pk_sales_transform.py","PythonVersion":"3"}' \
  --default-arguments '{
    "--extra-jars": "s3://pk-glue-assets-12345/jars/snowflake-jdbc-3.15.0.jar,s3://pk-glue-assets-12345/jars/spark-snowflake_2.12-2.12.0-spark_3.3.jar",
    "--TempDir": "s3://pk-glue-assets-12345/tmp/",
    "--job-language": "python",
    "--enable-continuous-cloudwatch-log": "true"
  }' \
  --glue-version "4.0" \
  --number-of-workers 5 \
  --worker-type "G.1X"
```

---

### Step 5: Amazon MSK (Kafka Cluster)

```bash
# Create MSK cluster (3 brokers, ap-south-1)
aws kafka create-cluster \
  --cluster-name "pk-sales-kafka" \
  --kafka-version "3.5.1" \
  --number-of-broker-nodes 3 \
  --broker-node-group-info '{
    "InstanceType": "kafka.m5.large",
    "ClientSubnets": ["subnet-aaa","subnet-bbb","subnet-ccc"],
    "SecurityGroups": ["sg-xxxxxxxx"],
    "StorageInfo": {"EbsStorageInfo": {"VolumeSize": 100}}
  }' \
  --encryption-info '{
    "EncryptionInTransit": {"ClientBroker": "TLS", "InCluster": true}
  }'

# Create topic after cluster is ACTIVE
aws kafka create-topic \
  --cluster-arn "arn:aws:kafka:ap-south-1:..." \
  --topic-name "pk-sales-data" \
  --partitions 6 \
  --replication-factor 3
```

---

### Step 6: Amazon MWAA (Managed Airflow)

```bash
# Upload DAG to S3
aws s3 cp aws_snowflake_data_eng/08_aws_python/airflow_dags/pk_sales_dag.py \
  s3://pk-airflow-dags-12345/dags/

# Upload requirements
cat > mwaa_requirements.txt << 'EOF'
apache-airflow-providers-snowflake==5.3.0
apache-airflow-providers-amazon==8.7.0
snowflake-connector-python[pandas]==3.6.0
EOF
aws s3 cp mwaa_requirements.txt s3://pk-airflow-dags-12345/requirements.txt

# Create MWAA environment
aws mwaa create-environment \
  --name "pk-data-platform" \
  --airflow-version "2.8.1" \
  --dag-s3-path "dags/" \
  --requirements-s3-path "requirements.txt" \
  --source-bucket-arn "arn:aws:s3:::pk-airflow-dags-12345" \
  --execution-role-arn "arn:aws:iam::ACCOUNT_ID:role/MWAARoleForPKPlatform" \
  --network-configuration '{"SubnetIds":["subnet-aaa","subnet-bbb"],"SecurityGroupIds":["sg-xxxxxxxx"]}' \
  --environment-class "mw1.small" \
  --max-workers 5 \
  --min-workers 1 \
  --logging-configuration '{
    "DagProcessingLogs": {"Enabled": true, "LogLevel": "WARNING"},
    "TaskLogs": {"Enabled": true, "LogLevel": "INFO"}
  }'
```

---

### Step 7: Lambda + API Gateway (External Functions)

```bash
# Package Lambda
zip lambda_currency.zip aws_snowflake_data_eng/07_external_functions/lambda_currency.py

# Create Lambda function
aws lambda create-function \
  --function-name "pk-currency-convert" \
  --runtime "python3.12" \
  --role "arn:aws:iam::ACCOUNT_ID:role/LambdaBasicRole" \
  --handler "lambda_currency.lambda_handler" \
  --zip-file fileb://lambda_currency.zip \
  --timeout 10 \
  --environment 'Variables={"SBP_API_KEY":"your_key_here"}'

# Create HTTP API
aws apigatewayv2 create-api \
  --name "pk-external-functions" \
  --protocol-type HTTP \
  --target "arn:aws:lambda:ap-south-1:ACCOUNT_ID:function:pk-currency-convert"
```

---

## 6. Snowflake Setup — Step by Step

### Step 1: Initial Database & Role Setup

Run `01_setup/initial_setup.sql` in a Snowflake worksheet:

```sql
-- Run as ACCOUNTADMIN
use role accountadmin;

-- Create service account for automation (no password — key-pair only)
create user svc_dataeng
  rsa_public_key = 'MIIBIjANBgkq...'   -- paste your public key
  default_role = sysadmin
  must_change_password = false;

-- Production warehouses
create warehouse compute_wh
  warehouse_size = 'X-SMALL'
  auto_suspend = 60
  auto_resume = true
  initially_suspended = true
  comment = 'ETL / Snowpipe workloads';

create warehouse prod_xl
  warehouse_size = 'LARGE'
  auto_suspend = 120
  auto_resume = true
  initially_suspended = true
  max_cluster_count = 3          -- multi-cluster for BI peak
  scaling_policy = 'ECONOMY'
  comment = 'BI query workloads';

-- Database + schemas
create database pk_ecommerce_db;
create schema pk_ecommerce_db.pk_ecommerce_raw;   -- raw ingest
create schema pk_ecommerce_db.pk_ecommerce_dev;   -- staging
create schema pk_ecommerce_db.pk_ecommerce_liv;   -- production

-- Grant structure
grant usage on database pk_ecommerce_db to role sysadmin;
grant all on schema pk_ecommerce_db.pk_ecommerce_liv to role sysadmin;
grant role sysadmin to user svc_dataeng;
```

---

### Step 2: Storage Integration (Snowflake ↔ S3)

```sql
-- Run as ACCOUNTADMIN
use role accountadmin;

CREATE OR REPLACE STORAGE INTEGRATION aws_pk_data
  TYPE                      = EXTERNAL_STAGE
  STORAGE_PROVIDER          = S3
  ENABLED                   = TRUE
  STORAGE_AWS_ROLE_ARN      = 'arn:aws:iam::ACCOUNT_ID:role/pk-snowflake-s3-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://pk-data-lake-prod-12345/');

-- Get Snowflake's AWS account ID and External ID to update IAM trust
DESC INTEGRATION aws_pk_data;
-- Copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID
-- Update the IAM trust policy with these values

grant usage on integration aws_pk_data to role sysadmin;
```

---

### Step 3: File Formats, Stages, Tables

```sql
use role sysadmin;
use schema pk_ecommerce_db.pk_ecommerce_dev;

-- File formats (from 03_data_loading/file_formats.sql)
CREATE OR REPLACE FILE FORMAT pk_csv_format
  TYPE='CSV' COMPRESSION='AUTO' FIELD_DELIMITER=',' SKIP_HEADER=1
  FIELD_OPTIONALLY_ENCLOSED_BY='\042' TRIM_SPACE=FALSE DATE_FORMAT='AUTO';

CREATE OR REPLACE FILE FORMAT pk_parquet_format TYPE='parquet';
CREATE OR REPLACE FILE FORMAT pk_json_format    TYPE='JSON';

-- External stages
CREATE OR REPLACE STAGE stg_orders
  STORAGE_INTEGRATION = aws_pk_data
  URL = 's3://pk-data-lake-prod-12345/pk_ecommerce_dev/orders/'
  FILE_FORMAT = pk_csv_format;

CREATE OR REPLACE STAGE stg_lineitems
  STORAGE_INTEGRATION = aws_pk_data
  URL = 's3://pk-data-lake-prod-12345/pk_ecommerce_dev/lineitems/'
  FILE_FORMAT = pk_csv_format;

-- Create production tables (from 01_setup/initial_setup.sql — already production SQL)
use schema pk_ecommerce_db.pk_ecommerce_liv;
-- (run initial_setup.sql here)
```

---

### Step 4: Snowpipe Auto-Ingest

```sql
use schema pk_ecommerce_db.pk_ecommerce_dev;

-- Create pipes
create or replace pipe pk_orders_pipe
  auto_ingest = true
  comment = 'Auto-ingest orders CSV from S3 on SNS notification'
as
  copy into pk_ecommerce_db.pk_ecommerce_liv.ORDERS
  from @stg_orders
  file_format = pk_csv_format
  on_error = continue;

create or replace pipe pk_lineitem_pipe
  auto_ingest = true
as
  copy into pk_ecommerce_db.pk_ecommerce_liv.LINEITEM
  from @stg_lineitems
  file_format = pk_csv_format
  on_error = continue;

-- Get the SQS ARN for SNS notification setup
show pipes;
-- Copy notification_channel value for each pipe
```

```bash
# Subscribe Snowpipe SQS to S3 SNS topic
aws sns subscribe \
  --topic-arn "arn:aws:sns:ap-south-1:ACCOUNT_ID:pk-s3-events" \
  --protocol sqs \
  --notification-endpoint "SNOWPIPE_SQS_ARN"
```

---

### Step 5: Streams & Tasks (CDC Pipeline)

```sql
use schema pk_ecommerce_db.pk_ecommerce_dev;
use role sysadmin;

-- Stream on raw JSON table (from 04_streams_cdc design)
CREATE OR REPLACE STREAM lineitem_std_stream
  ON TABLE pk_ecommerce_db.pk_ecommerce_raw.LINEITEM_RAW_JSON;

-- Task: runs every minute when stream has data
CREATE OR REPLACE TASK pk_lineitem_merge_task
  WAREHOUSE = compute_wh
  SCHEDULE  = '1 minute'
  WHEN system$stream_has_data('lineitem_std_stream')
AS
  MERGE INTO pk_ecommerce_db.pk_ecommerce_liv.LINEITEM AS li
  USING (
    SELECT SRC:L_ORDERKEY::NUMBER AS L_ORDERKEY, ...
    FROM lineitem_std_stream WHERE METADATA$ACTION='INSERT'
  ) AS src
  ON li.L_ORDERKEY = src.L_ORDERKEY AND li.L_LINENUMBER = src.L_LINENUMBER
  WHEN MATCHED THEN UPDATE SET ...
  WHEN NOT MATCHED THEN INSERT (...) VALUES (...);

ALTER TASK pk_lineitem_merge_task RESUME;
```

---

### Step 6: Masking & Row-Level Policies

```sql
use role accountadmin;
use schema pk_ecommerce_db.pk_ecommerce_liv;

-- Column masking: CNIC
CREATE OR REPLACE MASKING POLICY mask_cnic
  AS (val VARCHAR) RETURNS VARCHAR ->
  CASE
    WHEN CURRENT_ROLE() IN ('REPORTING_INTERN') THEN '***-*******-*'
    ELSE val
  END;

-- Column masking: Phone
CREATE OR REPLACE MASKING POLICY mask_phone
  AS (val VARCHAR) RETURNS VARCHAR ->
  CASE
    WHEN CURRENT_ROLE() IN ('REPORTING_INTERN') THEN '03**-*******'
    ELSE val
  END;

ALTER TABLE CUSTOMER MODIFY COLUMN C_CNIC   SET MASKING POLICY mask_cnic;
ALTER TABLE CUSTOMER MODIFY COLUMN C_PHONE  SET MASKING POLICY mask_phone;

-- Row-level access: province-based
CREATE TABLE access_management (role VARCHAR, province VARCHAR);
INSERT INTO access_management VALUES
  ('PUNJAB_REGIONAL_ADMIN', 'Punjab'),
  ('SINDH_REGIONAL_ADMIN',  'Sindh'),
  ('KPK_REGIONAL_ADMIN',    'KPK');

CREATE OR REPLACE ROW ACCESS POLICY province_access
  AS (province_filter VARCHAR) RETURNS BOOLEAN ->
  CURRENT_ROLE() IN ('ACCOUNTADMIN','SYSADMIN')
  OR EXISTS (
    SELECT 1 FROM access_management
    WHERE province = province_filter AND role = CURRENT_ROLE()
  );

ALTER TABLE CUSTOMER ADD ROW ACCESS POLICY province_access ON (C_PROVINCE);
ALTER TABLE ORDERS   ADD ROW ACCESS POLICY province_access ON (O_PROVINCE);
```

---

## 7. Connecting AWS to Snowflake

### Replace local connector with production connector

The POC uses `LocalSnowflakeConnection`. Replace it in all Python files:

```python
# BEFORE (POC — sqlite3)
from snowflake_connector import LocalSnowflakeConnection
conn = LocalSnowflakeConnection()

# AFTER (Production — real Snowflake with key-pair auth)
import snowflake.connector
import boto3, json
from cryptography.hazmat.primitives.serialization import load_pem_private_key

def get_snowflake_conn():
    # Fetch credentials from Secrets Manager
    sm = boto3.client('secretsmanager', region_name='ap-south-1')
    creds = json.loads(
        sm.get_secret_value(SecretId='prod/pk-platform/snowflake')['SecretString']
    )
    # Load private key
    with open(creds['private_key_path'], 'rb') as f:
        private_key = load_pem_private_key(f.read(), password=None)

    return snowflake.connector.connect(
        account   = creds['account'],
        user      = creds['user'],
        private_key = private_key,
        warehouse = creds['warehouse'],
        database  = creds['database'],
        schema    = 'pk_ecommerce_liv',
        role      = creds['role'],
        session_parameters = {'TIMEZONE': 'Asia/Karachi'},
    )
```

### Generate Snowflake key-pair

```bash
# Generate RSA key pair
openssl genrsa -out rsa_key.pem 2048
openssl rsa -in rsa_key.pem -pubout -out rsa_key.pub

# Extract public key (no header/footer) for Snowflake
grep -v "PUBLIC KEY" rsa_key.pub | tr -d '\n'

# In Snowflake worksheet
ALTER USER svc_dataeng SET RSA_PUBLIC_KEY='MIIBIjANBgkq...';

# Convert private key for snowflake connector
openssl pkcs8 -topk8 -inform PEM -outform DER -in rsa_key.pem -out rsa_key.p8 -nocrypt
```

### Airflow Snowflake connection

```bash
# In MWAA, create connection via CLI or UI
airflow connections add snowflake_pk_prod \
  --conn-type snowflake \
  --conn-host YOUR_ACCOUNT.ap-south-1.aws \
  --conn-login svc_dataeng \
  --conn-schema pk_ecommerce_liv \
  --conn-extra '{"account":"YOUR_ACCOUNT","warehouse":"COMPUTE_WH","database":"pk_ecommerce_db","role":"SYSADMIN","private_key_file":"/etc/snowflake/rsa_key.p8"}'
```

---

## 8. Airflow Deployment (MWAA)

### Update DAG for production

```python
# Replace the local simulation in pk_sales_dag.py with:
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

SNOWFLAKE_SQL = [
    "use role sysadmin;",
    "use schema pk_ecommerce_db.pk_ecommerce_dev;",
    "copy into pk_ecommerce_db.pk_ecommerce_liv.ORDERS from @stg_orders file_format=pk_csv_format on_error=continue;",
]

with dag:
    copy_task = SnowflakeOperator(
        task_id           = "pk_copy_orders",
        sql               = SNOWFLAKE_SQL,
        snowflake_conn_id = "snowflake_pk_prod",
    )

    glue_task = GlueJobOperator(
        task_id     = "pk_glue_transform",
        job_name    = "pk_sales_agg",
        aws_conn_id = "aws_default",
        script_args = {
            "--ship_date": "{{ ds }}",   # Airflow execution date
        },
        execution_timeout = timedelta(minutes=30),
    )

    copy_task >> glue_task
```

### Deploy DAG

```bash
# Sync DAG to S3 (triggers MWAA auto-reload)
aws s3 cp aws_snowflake_data_eng/08_aws_python/airflow_dags/pk_sales_dag.py \
  s3://pk-airflow-dags-12345/dags/ \
  --profile dataeng-prod

# Check DAG status
aws mwaa create-cli-token --name pk-data-platform --region ap-south-1
# Use token to call Airflow REST API to trigger DAG test run
```

---

## 9. Kafka Streaming (MSK)

### Snowflake Kafka Connector config (production version)

```properties
# SF_connect.properties — production
connector.class=com.snowflake.kafka.connector.SnowflakeSinkConnector
tasks.max=8
topics=pk-sales-data
snowflake.topic2table.map=pk-sales-data:SALES_DATA

buffer.count.records=10000
buffer.flush.time=60
buffer.size.bytes=5000000

snowflake.url.name=YOUR_ACCOUNT.ap-south-1.aws.snowflakecomputing.com
snowflake.user.name=svc_dataeng
snowflake.private.key=MIIEvQIBAD...    # base64-encoded private key
snowflake.database.name=pk_ecommerce_db
snowflake.schema.name=pk_ecommerce_dev
snowflake.role.name=SYSADMIN

key.converter=com.snowflake.kafka.connector.records.SnowflakeJsonConverter
value.converter=com.snowflake.kafka.connector.records.SnowflakeJsonConverter

name=pk_sales_sink
```

### Deploy connector to MSK Connect

```bash
# Upload connector plugin to S3
aws s3 cp snowflake-kafka-connector-2.1.0.jar \
  s3://pk-glue-assets-12345/kafka-plugins/

# Create MSK Connect connector
aws kafkaconnect create-connector \
  --connector-name "pk-sales-snowflake-sink" \
  --connector-configuration file://SF_connect.properties \
  --kafka-cluster '{"apacheKafkaCluster":{"bootstrapServers":"...","vpc":{...}}}' \
  --capacity '{"autoScaling":{"minWorkerCount":2,"maxWorkerCount":8,"scalingInPolicy":{"cpuUtilizationPercentage":20},"scalingOutPolicy":{"cpuUtilizationPercentage":80}}}'
```

### Update producer for production MSK

```python
# Replace LocalQueueProducer in pk_kafka_producer.py:
from kafka import KafkaProducer
import ssl

producer = KafkaProducer(
    bootstrap_servers = ['b-1.pk-sales-kafka.xxx.kafka.ap-south-1.amazonaws.com:9094'],
    security_protocol = 'SSL',
    ssl_cafile        = '/etc/kafka/ca-cert',
    value_serializer  = lambda x: json.dumps(x).encode('utf-8'),
    compression_type  = 'gzip',
    batch_size        = 16384,
    linger_ms         = 5,
)
```

---

## 10. Data Governance in Production

### Snowflake access hierarchy

```
ACCOUNTADMIN
├── SYSADMIN
│   ├── svc_dataeng          (service account — key-pair auth)
│   └── pk_dba               (human DBA — MFA required)
├── REPORTING_INTERN         (masked CNIC/phone, no raw tables)
├── PUNJAB_REGIONAL_ADMIN    (Punjab rows only)
├── SINDH_REGIONAL_ADMIN     (Sindh rows only)
└── TASK_OWNER               (execute tasks, no DDL)
```

### Dynamic Data Masking — full set for Pakistani PII

```sql
-- CNIC masking
CREATE MASKING POLICY mask_cnic AS (val VARCHAR) RETURNS VARCHAR ->
  CASE WHEN CURRENT_ROLE() IN ('REPORTING_INTERN') THEN '***-*******-*' ELSE val END;

-- Phone masking
CREATE MASKING POLICY mask_phone AS (val VARCHAR) RETURNS VARCHAR ->
  CASE WHEN CURRENT_ROLE() IN ('REPORTING_INTERN') THEN '03**-*******' ELSE val END;

-- Account balance masking
CREATE MASKING POLICY mask_balance AS (val NUMBER) RETURNS NUMBER ->
  CASE WHEN CURRENT_ROLE() IN ('REPORTING_INTERN') THEN -1 ELSE val END;
```

### SECP / PDPO Compliance (Pakistan Data Protection)
Pakistan's **Personal Data Protection Ordinance (PDPO)** and **SECP** regulations require:

| Requirement | Implementation |
|------------|----------------|
| Data localization | Snowflake AWS region `ap-south-1` (Mumbai) or `me-south-1` (Bahrain) |
| CNIC encryption | Dynamic masking + KMS-encrypted S3 |
| Consent tracking | Add `CONSENT_GIVEN` + `CONSENT_DATE` to CUSTOMER table |
| Right to deletion | Time-travel allows point-in-time deletes + audit |
| Audit logs | `SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY` + CloudTrail |

---

## 11. Monitoring & Alerting

### Snowflake monitoring queries

```sql
-- Credit consumption alert (run daily)
SELECT warehouse_name,
       SUM(credits_used)                      AS total_credits,
       SUM(credits_used * 3.28 * 278)         AS cost_pkr
FROM snowflake.account_usage.warehouse_metering_history
WHERE start_time >= dateadd('day', -1, current_timestamp())
GROUP BY 1
ORDER BY total_credits DESC;

-- Long-running queries (> 5 min)
SELECT query_id, user_name, warehouse_name,
       execution_time / 1000 / 60             AS exec_min,
       query_text
FROM snowflake.account_usage.query_history
WHERE execution_time > 300000
  AND start_time >= dateadd('hour', -1, current_timestamp())
ORDER BY exec_min DESC;

-- Failed pipes (Snowpipe errors)
SELECT pipe_name, file_name, error_message, first_error_message
FROM snowflake.account_usage.copy_history
WHERE status = 'LOAD_FAILED'
  AND last_load_time >= dateadd('hour', -1, current_timestamp());

-- Task failure history
SELECT name, state, error_message, scheduled_time
FROM snowflake.account_usage.task_history
WHERE state = 'FAILED'
  AND scheduled_time >= dateadd('hour', -1, current_timestamp());
```

### CloudWatch alarms

```bash
# S3 put failure alarm
aws cloudwatch put-metric-alarm \
  --alarm-name "pk-s3-put-failures" \
  --metric-name "5xxError" \
  --namespace "AWS/S3" \
  --statistic Sum --period 300 --threshold 1 \
  --comparison-operator GreaterThanOrEqualToThreshold \
  --evaluation-periods 1 \
  --alarm-actions "arn:aws:sns:ap-south-1:ACCOUNT_ID:pk-alerts"

# Glue job failure alarm
aws cloudwatch put-metric-alarm \
  --alarm-name "pk-glue-job-failure" \
  --metric-name "glue.driver.aggregate.numFailedTasks" \
  --namespace "Glue" \
  --statistic Sum --period 300 --threshold 1 \
  --comparison-operator GreaterThanOrEqualToThreshold \
  --evaluation-periods 1 \
  --alarm-actions "arn:aws:sns:ap-south-1:ACCOUNT_ID:pk-alerts"
```

### SNS alert notification (SMS to Pakistani number)

```bash
aws sns subscribe \
  --topic-arn "arn:aws:sns:ap-south-1:ACCOUNT_ID:pk-alerts" \
  --protocol sms \
  --notification-endpoint "+92300XXXXXXX"   # Pakistani mobile
```

---

## 12. Cost Estimate (PKR / USD)

> Estimates for medium-scale Pakistani ecommerce platform: 2M orders/month, 10M lineitems.
> PKR rate: 278 USD

| Service | Config | USD/month | PKR/month |
|---------|--------|-----------|-----------|
| **Snowflake** | Enterprise, 2 warehouses (XS + L), 1TB storage | ~$800 | ~₨222,400 |
| **Amazon S3** | 500GB data lake + transfers | ~$12 | ~₨3,336 |
| **Amazon MSK** | 3 × kafka.m5.large + 300GB EBS | ~$220 | ~₨61,160 |
| **Amazon MWAA** | mw1.small, 5 workers | ~$300 | ~₨83,400 |
| **AWS Glue** | 5 DPUs × 2h/day × 30 days | ~$45 | ~₨12,510 |
| **AWS Lambda** | 1M invocations, 256MB | ~$5 | ~₨1,390 |
| **API Gateway** | 1M calls | ~$3 | ~₨834 |
| **Secrets Manager** | 10 secrets | ~$4 | ~₨1,112 |
| **CloudWatch** | Logs + metrics | ~$15 | ~₨4,170 |
| **Apache Superset** | EC2 t3.medium | ~$30 | ~₨8,340 |
| **Data Transfer** | 100GB out | ~$9 | ~₨2,502 |
| **Total** | | **~$1,443/mo** | **~₨401,154/mo** |

### Cost reduction tips
- Snowflake: Use `auto_suspend = 60s` on all warehouses — saves 40–60%
- MSK: Use `kafka.t3.small` for dev/staging environments
- Glue: Run jobs only when stream data exists (condition in Airflow)
- S3: Enable Intelligent-Tiering after 30 days

---

## 13. Security Hardening Checklist

### AWS
- [ ] Enable CloudTrail in all regions (write to S3, 7-year retention)
- [ ] Enable AWS Config — detect drift in IAM/S3 policies
- [ ] Enable GuardDuty — threat detection
- [ ] S3 Block Public Access — ON for all buckets
- [ ] S3 Bucket Policy — deny non-HTTPS requests
- [ ] IAM — no user access keys; use roles everywhere
- [ ] Rotate Secrets Manager secrets every 90 days
- [ ] Enable VPC Flow Logs for MSK subnet
- [ ] Enable MFA Delete on S3 data buckets
- [ ] Snowflake network policy — restrict to VPC CIDR only

### Snowflake
- [ ] `svc_dataeng` — key-pair auth only (no password)
- [ ] Human accounts — MFA enforced (`ALTER USER ... SET MFA_TYPE='DUO'`)
- [ ] Network policy: `CREATE NETWORK POLICY pk_vpc_only ALLOWED_IP_LIST=('10.0.0.0/16')`
- [ ] `PREVENT_UNLOAD_TO_INLINE_URL = TRUE` — stop data export to unsigned URLs
- [ ] Enable Snowflake Access History auditing
- [ ] `REQUIRE_STORAGE_INTEGRATION_FOR_STAGE_CREATION = TRUE`
- [ ] Masking policies applied to all PII columns (CNIC, phone, address)
- [ ] Data Retention = 30 days for production, 7 days for dev

### Application
- [ ] No credentials in code — all from Secrets Manager
- [ ] Parameterized SQL only — no string interpolation in queries
- [ ] Dependency pinning in `requirements.txt`
- [ ] `pip audit` in CI/CD pipeline
- [ ] SAST scan (Bandit for Python) in GitHub Actions

---

## 14. Disaster Recovery

### RPO / RTO targets

| Layer | RPO | RTO | Mechanism |
|-------|-----|-----|-----------|
| Snowflake data | 1 hour | 4 hours | Time-travel (30d) + Fail-safe (7d) |
| S3 raw files | 24 hours | 2 hours | Cross-region replication to me-south-1 |
| MSK | 1 hour | 2 hours | Multi-AZ + 7-day retention |
| MWAA DAGs | 0 (code in S3) | 30 min | S3 versioning |
| Glue scripts | 0 (code in Git) | 15 min | Git + S3 |

### Snowflake time-travel restore

```sql
-- Recover accidentally deleted orders (within 30 days)
CREATE TABLE ORDERS_RECOVERED CLONE ORDERS
  AT (TIMESTAMP => '2024-11-15 08:30:00'::TIMESTAMP_TZ);

-- Validate
SELECT count(1) FROM ORDERS_RECOVERED;

-- Swap in
ALTER TABLE ORDERS RENAME TO ORDERS_BAD;
ALTER TABLE ORDERS_RECOVERED RENAME TO ORDERS;
DROP TABLE ORDERS_BAD;
```

### S3 cross-region replication

```bash
aws s3api put-bucket-replication \
  --bucket pk-data-lake-prod-12345 \
  --replication-configuration '{
    "Role": "arn:aws:iam::ACCOUNT_ID:role/S3ReplicationRole",
    "Rules": [{
      "Status": "Enabled",
      "Destination": {
        "Bucket": "arn:aws:s3:::pk-data-lake-dr-12345",
        "StorageClass": "STANDARD_IA"
      }
    }]
  }'
```

---

## 15. Go-Live Checklist

### Phase 1 — Infrastructure (Week 1)
- [ ] S3 buckets created + encrypted + versioned
- [ ] IAM roles created (Snowflake, Glue, MWAA, Lambda)
- [ ] Secrets Manager populated
- [ ] Snowflake account purchased in `ap-south-1`
- [ ] Snowflake key-pair auth configured for `svc_dataeng`
- [ ] Storage integration created + IAM trust updated
- [ ] VPC / security groups configured

### Phase 2 — Data Platform (Week 2)
- [ ] Snowflake DDL executed (`initial_setup.sql`)
- [ ] File formats + stages created (`load_data.sql`)
- [ ] COPY INTO tested manually with sample CSV
- [ ] Snowpipe + SNS notifications configured
- [ ] CDC streams + Tasks deployed and tested
- [ ] Masking policies applied to CNIC, phone
- [ ] Row-level policies applied to CUSTOMER, ORDERS

### Phase 3 — Streaming & ETL (Week 3)
- [ ] MSK cluster running, topic `pk-sales-data` created
- [ ] Snowflake Kafka Connector deployed + topic-to-table mapping verified
- [ ] Glue job `pk_sales_agg` running end-to-end
- [ ] MWAA environment running, DAG `pk_sales_pipeline` scheduled
- [ ] Lambda `pk-currency-convert` deployed + tested
- [ ] API Gateway → Lambda → Snowflake External Function tested

### Phase 4 — Observability & Go-Live (Week 4)
- [ ] CloudWatch alarms configured
- [ ] SNS SMS alerts to `+92300XXXXXXX` verified
- [ ] Apache Superset deployed + connected to Snowflake
- [ ] Snowflake dashboard charts verified with live data
- [ ] Load test: 1000 concurrent orders
- [ ] DR drill: restore from time-travel point
- [ ] Security review: IAM least-privilege audit
- [ ] Cost alarm: Budget alert at $1,200/month
- [ ] Runbook documented and shared with team

---

## Quick Reference — Key Commands

```bash
# Run full local pipeline
python aws_snowflake_data_eng/run_pipeline.py

# Start Streamlit dashboard
streamlit run aws_snowflake_data_eng/superset_dashboard.py

# Generate fresh Pakistani data
python aws_snowflake_data_eng/02_data_generator/pk_data_generator.py

# Check DB table counts
python aws_snowflake_data_eng/check_db.py

# Stream 100 Kafka events (local)
python aws_snowflake_data_eng/09_kafka_streaming/pk_kafka_producer.py 100

# Upload data to S3 (production)
aws s3 sync aws_snowflake_data_eng/local_s3/pk_ecommerce_dev/ \
  s3://pk-data-lake-prod-12345/pk_ecommerce_dev/ \
  --profile dataeng-prod
```

---

*Document version: 1.0 · Platform: AWS ap-south-1 + Snowflake Enterprise · Target: Pakistani Ecommerce Data Platform*
