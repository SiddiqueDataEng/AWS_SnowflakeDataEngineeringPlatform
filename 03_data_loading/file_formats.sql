-- ============================================================
-- 03_data_loading/file_formats.sql
-- Snowflake file format definitions for Pakistani data platform
-- ============================================================

use role sysadmin;
use schema pk_ecommerce_db.pk_ecommerce_dev;

-- CSV — standard Pakistani CSV export (UTF-8, comma-delimited)
CREATE OR REPLACE FILE FORMAT pk_csv_format
    TYPE                         = 'CSV'
    COMPRESSION                  = 'AUTO'
    FIELD_DELIMITER              = ','
    RECORD_DELIMITER             = '\n'
    SKIP_HEADER                  = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '\042'
    TRIM_SPACE                   = FALSE
    ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
    ESCAPE                       = 'NONE'
    ESCAPE_UNENCLOSED_FIELD      = '\134'
    DATE_FORMAT                  = 'AUTO'
    TIMESTAMP_FORMAT             = 'AUTO';

-- Parquet — for columnar analytics
CREATE OR REPLACE FILE FORMAT pk_parquet_format
    TYPE = 'parquet';

-- JSON — for streaming / Snowpipe ingestion
CREATE OR REPLACE FILE FORMAT pk_json_format
    TYPE = 'JSON';
