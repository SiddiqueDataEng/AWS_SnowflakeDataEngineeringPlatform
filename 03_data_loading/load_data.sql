-- ============================================================
-- 03_data_loading/load_data.sql
-- AWS S3 → Snowflake: storage integration, stages, COPY INTO
-- (Replace bucket placeholders with your actual S3 bucket)
-- ============================================================

use role accountadmin;

-- Step 1: Storage Integration (IAM trust between Snowflake & S3)
CREATE OR REPLACE STORAGE INTEGRATION aws_pk_data
    TYPE                    = EXTERNAL_STAGE
    STORAGE_PROVIDER        = S3
    ENABLED                 = TRUE
    STORAGE_AWS_ROLE_ARN    = '{your_iam_role_arn}'
    STORAGE_ALLOWED_LOCATIONS = ('{your_bucket_name}/pk_ecommerce_dev/');

desc INTEGRATION aws_pk_data;
grant usage on integration aws_pk_data to role sysadmin;
grant create stage on schema pk_ecommerce_db.pk_ecommerce_dev to role sysadmin;

use role sysadmin;
use schema pk_ecommerce_db.pk_ecommerce_dev;

-- Step 2: Create external stages pointing to S3 paths
create or replace stage stg_customers
    storage_integration = aws_pk_data
    url = '{your_bucket_name}/pk_ecommerce_dev/customers/'
    file_format = pk_csv_format;

create or replace stage stg_orders
    storage_integration = aws_pk_data
    url = '{your_bucket_name}/pk_ecommerce_dev/orders/'
    file_format = pk_csv_format;

create or replace stage stg_lineitems
    storage_integration = aws_pk_data
    url = '{your_bucket_name}/pk_ecommerce_dev/lineitems/'
    file_format = pk_csv_format;

create or replace stage stg_lineitems_json
    storage_integration = aws_pk_data
    url = '{your_bucket_name}/pk_ecommerce_dev/lineitems/'
    file_format = pk_json_format;

-- List staged files before loading
list @stg_customers;
list @stg_orders;
list @stg_lineitems;

-- Step 3: COPY INTO — bulk load Pakistani data
copy into CUSTOMER from @stg_customers    ON_ERROR = CONTINUE;
copy into ORDERS   from @stg_orders       ON_ERROR = CONTINUE;
copy into LINEITEM from @stg_lineitems    ON_ERROR = CONTINUE;

-- Step 4: Validate loaded data
select count(1), 'CUSTOMER' as tbl from CUSTOMER
union all
select count(1), 'ORDERS'   from ORDERS
union all
select count(1), 'LINEITEM' from LINEITEM;

-- Step 5: Check load history
select * from information_schema.load_history
where table_name in ('CUSTOMER','ORDERS','LINEITEM')
order by last_load_time desc limit 20;

-- ── Snowpipe: auto-ingest from S3 on new file arrival ─────────────────────────
create or replace pipe pk_orders_pipe auto_ingest = true as
    copy into ORDERS from @stg_orders ON_ERROR = continue;

create or replace pipe pk_lineitem_pipe auto_ingest = true as
    copy into LINEITEM from @stg_lineitems ON_ERROR = continue;

show pipes;
