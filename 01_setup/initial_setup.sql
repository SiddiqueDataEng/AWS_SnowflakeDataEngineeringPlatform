-- ============================================================
-- 01_setup/initial_setup.sql
-- Pakistani Ecommerce Data Platform — Snowflake Initial Setup
-- (Simulated locally via SQLite in local_db/pk_ecommerce.db)
-- ============================================================

-- Roles & Warehouse (Snowflake commands — documented for reference)
use role sysadmin;

create database if not exists pk_ecommerce_db;

create schema if not exists pk_ecommerce_liv;   -- Live / production
create schema if not exists pk_ecommerce_dev;   -- Development / staging

use schema pk_ecommerce_db.pk_ecommerce_liv;

use warehouse compute_wh;

-- ── Core tables (Pakistani retail domain) ─────────────────────────────────────

create or replace table CUSTOMER (
    C_CUSTKEY      number        primary key,
    C_NAME         varchar(100)  not null,
    C_CNIC         varchar(20)   unique,          -- Pakistani National ID: DDDDD-DDDDDDD-D
    C_PHONE        varchar(20),
    C_GENDER       char(1),
    C_CITY         varchar(50),
    C_PROVINCE     varchar(50),
    C_ADDRESS      varchar(200),
    C_ACCTBAL      numeric(15,2),
    C_COMMENT      varchar(500)
);

create or replace table SUPPLIER (
    S_SUPPKEY      number        primary key,
    S_NAME         varchar(100)  not null,
    S_ADDRESS      varchar(200),
    S_CITY         varchar(50),
    S_PROVINCE     varchar(50),
    S_PHONE        varchar(20),
    S_ACCTBAL      numeric(15,2),
    S_COMMENT      varchar(500)
);

create or replace table PART (
    P_PARTKEY      number        primary key,
    P_NAME         varchar(150)  not null,
    P_CATEGORY     varchar(50),
    P_BRAND        varchar(50),
    P_TYPE         varchar(30),
    P_SIZE         number,
    P_RETAILPRICE  numeric(12,2),
    P_COMMENT      varchar(500)
);

create or replace table ORDERS (
    O_ORDERKEY     number        primary key,
    O_CUSTKEY      number        references CUSTOMER(C_CUSTKEY),
    O_ORDERSTATUS  char(1),      -- P=Processing, O=Open, F=Fulfilled
    O_TOTALPRICE   numeric(15,2),
    O_ORDERDATE    date,
    O_ORDERPRIORITY varchar(20),
    O_CLERK        varchar(20),
    O_SHIPPRIORITY number,
    O_COMMENT      varchar(500)
);

create or replace table LINEITEM (
    L_ORDERKEY     number        references ORDERS(O_ORDERKEY),
    L_PARTKEY      number        references PART(P_PARTKEY),
    L_SUPPKEY      number        references SUPPLIER(S_SUPPKEY),
    L_LINENUMBER   number,
    L_QUANTITY     numeric(10,2),
    L_EXTENDEDPRICE numeric(15,2),
    L_DISCOUNT     numeric(5,2),
    L_TAX          numeric(5,2),  -- Pakistan GST (0%, 5%, or 17%)
    L_RETURNFLAG   char(1),
    L_LINESTATUS   char(1),
    L_SHIPDATE     date,
    L_COMMITDATE   date,
    L_RECEIPTDATE  date,
    L_SHIPINSTRUCT varchar(50),
    L_SHIPMODE     varchar(10),
    L_COURIER      varchar(30),   -- TCS / Leopards / M&P / Pakistan Post / Trax / Swyft
    L_COMMENT      varchar(500),
    primary key (L_ORDERKEY, L_LINENUMBER)
);

-- ── Clustering keys (Snowflake-specific) ─────────────────────────────────────
alter table LINEITEM cluster by (L_SHIPDATE);
alter table ORDERS   cluster by (O_ORDERDATE);

-- ── Data retention for time travel ───────────────────────────────────────────
alter table LINEITEM set data_retention_time_in_days = 30;
alter table ORDERS   set data_retention_time_in_days = 30;
