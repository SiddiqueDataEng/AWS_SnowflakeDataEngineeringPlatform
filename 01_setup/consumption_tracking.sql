-- ============================================================
-- 01_setup/consumption_tracking.sql
-- Monitor Snowflake warehouse credits & query execution costs
-- (Pakistan ecommerce platform)
-- ============================================================

use role accountadmin;

-- ── Warehouse credit usage (cost in PKR @ ~3.28 USD/credit * PKR rate) ───────
select
    round(sum(credits_used * 3.28 * 278), 2)          as billed_amount_pkr,
    warehouse_name,
    TIMESTAMPDIFF('minute', start_time, end_time)      as run_time_minutes,
    date(start_time)                                   as execution_date,
    hour(start_time)                                   as execution_hour
from snowflake.account_usage.warehouse_metering_history
group by 2, 3, 4, 5
order by execution_date desc;

-- ── Query execution bucketed by duration ─────────────────────────────────────
select
    count(distinct query_id)                           as total_queries,
    case
        when round(execution_time / 6000) between 0  and 10  then '0-10 min'
        when round(execution_time / 6000) between 10 and 20  then '10-20 min'
        when round(execution_time / 6000) between 20 and 50  then '20-50 min'
        when round(execution_time / 6000) > 50               then '50+ min'
    end                                                as exec_time_bucket,
    nvl(warehouse_name, 'Result Cache')                as warehouse,
    user_name,
    date(start_time)                                   as exec_date
from snowflake.account_usage.query_history
group by 2, 3, 4, 5
order by exec_date desc;
