-- ============================================================
-- Cost & Usage Monitoring Queries
-- Run these queries to monitor pipeline costs and performance
-- ============================================================

-- ============================================================
-- SECTION 1: CURRENT COSTS
-- ============================================================

-- Query 0: Hourly Cost Breakdown (Last 48 Hours)
SELECT 
    DATE_TRUNC('HOUR', usage_start_time) as usage_hour,
    SUM(usage_quantity * list_price) as cost_usd,
    COUNT(DISTINCT usage_metadata.job_id) as job_count,
    SUM(usage_quantity) as total_dbus
FROM system.billing.usage
WHERE usage_start_time >= CURRENT_TIMESTAMP - INTERVAL 48 HOURS
    AND usage_metadata.job_name LIKE '%Tix Master%'
GROUP BY 1
ORDER BY usage_hour DESC;

-- Query 0B: Hourly Cost by Task (Last 24 Hours)
SELECT 
    DATE_TRUNC('HOUR', usage_start_time) as usage_hour,
    COALESCE(usage_metadata.task_key, 'unknown') as task_name,
    SUM(usage_quantity * list_price) as cost_usd,
    SUM(usage_quantity) as dbus_consumed
FROM system.billing.usage
WHERE usage_start_time >= CURRENT_TIMESTAMP - INTERVAL 24 HOURS
    AND usage_metadata.job_name LIKE '%Tix Master%'
GROUP BY 1, 2
ORDER BY usage_hour DESC, cost_usd DESC;

-- Query 0C: Current Hour Cost (Real-time)
SELECT 
    DATE_TRUNC('HOUR', CURRENT_TIMESTAMP) as current_hour,
    COALESCE(SUM(usage_quantity * list_price), 0) as cost_usd,
    COALESCE(SUM(usage_quantity), 0) as dbus_consumed,
    COUNT(DISTINCT usage_metadata.task_key) as active_tasks
FROM system.billing.usage
WHERE DATE_TRUNC('HOUR', usage_start_time) = DATE_TRUNC('HOUR', CURRENT_TIMESTAMP)
    AND usage_metadata.job_name LIKE '%Tix Master%';

-- Query 1: Today's Pipeline Costs
SELECT 
    DATE(usage_start_time) as usage_date,
    usage_metadata.job_name,
    usage_metadata.task_key,
    SUM(usage_quantity) as total_dbus,
    SUM(usage_quantity * list_price) as cost_usd
FROM system.billing.usage
WHERE 
    DATE(usage_start_time) = CURRENT_DATE
    AND usage_metadata.job_name LIKE '%Tix Master%'
GROUP BY 1, 2, 3
ORDER BY cost_usd DESC;

-- Query 2: Last 7 Days Cost Breakdown
SELECT 
    DATE(usage_start_time) as usage_date,
    sku_name,
    SUM(usage_quantity) as total_dbus,
    SUM(usage_quantity * list_price) as cost_usd
FROM system.billing.usage
WHERE 
    usage_start_time >= CURRENT_TIMESTAMP - INTERVAL 7 DAYS
    AND usage_metadata.job_name LIKE '%Tix Master%'
GROUP BY 1, 2
ORDER BY usage_date DESC, cost_usd DESC;

-- Query 3: Monthly Cost Summary
SELECT 
    DATE_TRUNC('MONTH', usage_start_time) as month,
    SUM(usage_quantity) as total_dbus,
    SUM(usage_quantity * list_price) as total_cost_usd,
    AVG(usage_quantity * list_price) as avg_daily_cost
FROM system.billing.usage
WHERE 
    usage_start_time >= DATE_TRUNC('MONTH', CURRENT_DATE - INTERVAL 3 MONTHS)
    AND usage_metadata.job_name LIKE '%Tix Master%'
GROUP BY 1
ORDER BY month DESC;

-- Query 4: Cost by Layer (Bronze/Silver/Gold)
SELECT 
    CASE 
        WHEN usage_metadata.task_key LIKE '%bronze%' THEN 'BRONZE'
        WHEN usage_metadata.task_key LIKE '%silver%' THEN 'SILVER'
        WHEN usage_metadata.task_key LIKE '%gold%' THEN 'GOLD'
        WHEN usage_metadata.task_key LIKE '%ingest%' THEN 'INGESTION'
        ELSE 'OTHER'
    END as layer,
    DATE(usage_start_time) as usage_date,
    SUM(usage_quantity) as total_dbus,
    SUM(usage_quantity * list_price) as cost_usd
FROM system.billing.usage
WHERE 
    usage_start_time >= CURRENT_DATE - INTERVAL 30 DAYS
    AND usage_metadata.job_name LIKE '%Tix Master%'
GROUP BY 1, 2
ORDER BY usage_date DESC, cost_usd DESC;

-- ============================================================
-- SECTION 2: JOB PERFORMANCE
-- ============================================================

-- Query 5: Recent Job Runs
SELECT 
    run_id,
    start_time,
    end_time,
    TIMESTAMPDIFF(MINUTE, start_time, end_time) as duration_minutes,
    state,
    result_state
FROM system.lakeflow.job_runs
WHERE 
    job_name LIKE '%Tix Master ETL Pipeline%'
    AND start_time >= CURRENT_TIMESTAMP - INTERVAL 7 DAYS
ORDER BY start_time DESC
LIMIT 50;

-- Query 6: Job Success Rate (Last 30 Days)
SELECT 
    DATE(start_time) as run_date,
    COUNT(*) as total_runs,
    SUM(CASE WHEN result_state = 'SUCCESS' THEN 1 ELSE 0 END) as successful_runs,
    SUM(CASE WHEN result_state = 'FAILED' THEN 1 ELSE 0 END) as failed_runs,
    ROUND(100.0 * SUM(CASE WHEN result_state = 'SUCCESS' THEN 1 ELSE 0 END) / COUNT(*), 2) as success_rate_pct
FROM system.lakeflow.job_runs
WHERE 
    job_name LIKE '%Tix Master ETL Pipeline%'
    AND start_time >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY 1
ORDER BY run_date DESC;

-- Query 7: Average Job Duration Trend
SELECT 
    DATE(start_time) as run_date,
    AVG(TIMESTAMPDIFF(MINUTE, start_time, end_time)) as avg_duration_minutes,
    MIN(TIMESTAMPDIFF(MINUTE, start_time, end_time)) as min_duration_minutes,
    MAX(TIMESTAMPDIFF(MINUTE, start_time, end_time)) as max_duration_minutes,
    COUNT(*) as run_count
FROM system.lakeflow.job_runs
WHERE 
    job_name LIKE '%Tix Master ETL Pipeline%'
    AND start_time >= CURRENT_DATE - INTERVAL 30 DAYS
    AND result_state = 'SUCCESS'
GROUP BY 1
ORDER BY run_date DESC;

-- Query 8: Task-Level Performance
SELECT 
    task_key,
    COUNT(*) as executions,
    AVG(execution_duration) / 1000 / 60 as avg_duration_minutes,
    MAX(execution_duration) / 1000 / 60 as max_duration_minutes,
    SUM(CASE WHEN state = 'FAILED' THEN 1 ELSE 0 END) as failures
FROM system.lakeflow.task_runs tr
JOIN system.lakeflow.job_runs jr ON tr.run_id = jr.run_id
WHERE 
    jr.job_name LIKE '%Tix Master ETL Pipeline%'
    AND tr.start_time >= CURRENT_DATE - INTERVAL 7 DAYS
GROUP BY 1
ORDER BY avg_duration_minutes DESC;

-- ============================================================
-- SECTION 3: DATA VOLUME METRICS
-- ============================================================

-- Query 9: Table Sizes by Layer
SELECT 
    table_catalog,
    table_schema,
    table_name,
    ROUND(SUM(total_size_bytes) / 1024 / 1024 / 1024, 2) as size_gb,
    MAX(last_altered) as last_modified
FROM system.information_schema.tables t
LEFT JOIN system.information_schema.table_storage_stats ts 
    ON t.table_catalog = ts.table_catalog 
    AND t.table_schema = ts.table_schema 
    AND t.table_name = ts.table_name
WHERE 
    t.table_catalog = 'ticket_master'
    AND t.table_schema IN ('bronze', 'silver', 'gold')
    AND t.table_type = 'MANAGED'
GROUP BY 1, 2, 3
ORDER BY size_gb DESC;

-- Query 10: Row Counts by Layer
SELECT 
    'bronze' as layer,
    'events_raw' as table_name,
    COUNT(*) as row_count,
    MAX(_ingestion_timestamp) as last_ingestion
FROM ticket_master.bronze.events_raw

UNION ALL

SELECT 
    'silver' as layer,
    'events' as table_name,
    COUNT(*) as row_count,
    MAX(created_at) as last_ingestion
FROM ticket_master.silver.events

UNION ALL

SELECT 
    'gold' as layer,
    'fact_events' as table_name,
    COUNT(*) as row_count,
    NULL as last_ingestion
FROM ticket_master.gold.fact_events

ORDER BY layer, table_name;

-- Query 11: Daily Data Growth
SELECT 
    DATE(_ingestion_timestamp) as ingestion_date,
    COUNT(*) as events_ingested,
    COUNT(DISTINCT _source_file) as files_processed
FROM ticket_master.bronze.events_raw
WHERE _ingestion_timestamp >= CURRENT_TIMESTAMP - INTERVAL 30 DAYS
GROUP BY 1
ORDER BY ingestion_date DESC;

-- ============================================================
-- SECTION 4: COST ALERTS
-- ============================================================

-- Query 12: Daily Cost vs. Budget (Set your threshold)
WITH daily_costs AS (
    SELECT 
        DATE(usage_start_time) as usage_date,
        SUM(usage_quantity * list_price) as daily_cost
    FROM system.billing.usage
    WHERE 
        usage_metadata.job_name LIKE '%Tix Master%'
        AND usage_start_time >= CURRENT_DATE - INTERVAL 30 DAYS
    GROUP BY 1
)
SELECT 
    usage_date,
    daily_cost,
    50.00 as daily_budget_threshold,  -- Set your threshold here
    CASE 
        WHEN daily_cost > 50.00 THEN '⚠️ OVER BUDGET'
        WHEN daily_cost > 40.00 THEN '⚡ WARNING'
        ELSE '✅ OK'
    END as status
FROM daily_costs
ORDER BY usage_date DESC;

-- Query 13: Cost Spike Detection (>50% increase from average)
WITH daily_costs AS (
    SELECT 
        DATE(usage_start_time) as usage_date,
        SUM(usage_quantity * list_price) as daily_cost
    FROM system.billing.usage
    WHERE 
        usage_metadata.job_name LIKE '%Tix Master%'
        AND usage_start_time >= CURRENT_DATE - INTERVAL 30 DAYS
    GROUP BY 1
),
avg_cost AS (
    SELECT AVG(daily_cost) as avg_daily_cost
    FROM daily_costs
)
SELECT 
    dc.usage_date,
    dc.daily_cost,
    ac.avg_daily_cost,
    ROUND(100.0 * (dc.daily_cost - ac.avg_daily_cost) / ac.avg_daily_cost, 2) as pct_change,
    CASE 
        WHEN dc.daily_cost > ac.avg_daily_cost * 1.5 THEN '🚨 SPIKE DETECTED'
        WHEN dc.daily_cost > ac.avg_daily_cost * 1.2 THEN '⚠️ ABOVE AVERAGE'
        ELSE '✅ NORMAL'
    END as alert_status
FROM daily_costs dc
CROSS JOIN avg_cost ac
WHERE dc.usage_date >= CURRENT_DATE - INTERVAL 7 DAYS
ORDER BY dc.usage_date DESC;

-- ============================================================
-- SECTION 5: POPULATE MONITORING TABLES
-- ============================================================

-- Query 14: Refresh Pipeline Cost Metrics (Run Daily)
INSERT INTO ticket_master.gold.pipeline_cost_metrics 
  (run_date, job_name, task_name, dbus_consumed, estimated_cost_usd)
SELECT 
    DATE(usage_start_time) as run_date,
    usage_metadata.job_name,
    usage_metadata.task_key,
    SUM(usage_quantity) as dbus_consumed,
    SUM(usage_quantity * list_price) as estimated_cost_usd
FROM system.billing.usage
WHERE 
    DATE(usage_start_time) = CURRENT_DATE - 1  -- Yesterday's data
    AND usage_metadata.job_name LIKE '%Tix Master%'
GROUP BY 1, 2, 3;

-- Query 15: Refresh Job Execution Metrics (Run Daily)
INSERT INTO ticket_master.gold.job_execution_metrics 
  (job_id, run_id, job_name, start_time, end_time, duration_minutes, status)
SELECT 
    job_id,
    run_id,
    job_name,
    start_time,
    end_time,
    TIMESTAMPDIFF(MINUTE, start_time, end_time) as duration_minutes,
    result_state as status
FROM system.lakeflow.job_runs
WHERE 
    job_name LIKE '%Tix Master ETL Pipeline%'
    AND DATE(start_time) = CURRENT_DATE - 1
    AND run_id NOT IN (SELECT run_id FROM ticket_master.gold.job_execution_metrics);

-- ============================================================
-- SECTION 6: DASHBOARD QUERIES
-- ============================================================

-- Query 16: Cost Dashboard Summary (Use for dashboard)
SELECT 
    'Today' as period,
    COALESCE(SUM(usage_quantity * list_price), 0) as cost_usd
FROM system.billing.usage
WHERE DATE(usage_start_time) = CURRENT_DATE
    AND usage_metadata.job_name LIKE '%Tix Master%'

UNION ALL

SELECT 
    'Yesterday' as period,
    COALESCE(SUM(usage_quantity * list_price), 0) as cost_usd
FROM system.billing.usage
WHERE DATE(usage_start_time) = CURRENT_DATE - 1
    AND usage_metadata.job_name LIKE '%Tix Master%'

UNION ALL

SELECT 
    'Last 7 Days' as period,
    COALESCE(SUM(usage_quantity * list_price), 0) as cost_usd
FROM system.billing.usage
WHERE usage_start_time >= CURRENT_DATE - INTERVAL 7 DAYS
    AND usage_metadata.job_name LIKE '%Tix Master%'

UNION ALL

SELECT 
    'Last 30 Days' as period,
    COALESCE(SUM(usage_quantity * list_price), 0) as cost_usd
FROM system.billing.usage
WHERE usage_start_time >= CURRENT_DATE - INTERVAL 30 DAYS
    AND usage_metadata.job_name LIKE '%Tix Master%';

-- Query 17: Performance Dashboard Summary
SELECT 
    'Total Runs (30d)' as metric,
    COUNT(*)::STRING as value
FROM system.lakeflow.job_runs
WHERE job_name LIKE '%Tix Master ETL Pipeline%'
    AND start_time >= CURRENT_DATE - INTERVAL 30 DAYS

UNION ALL

SELECT 
    'Success Rate (30d)' as metric,
    CONCAT(ROUND(100.0 * SUM(CASE WHEN result_state = 'SUCCESS' THEN 1 ELSE 0 END) / COUNT(*), 1), '%') as value
FROM system.lakeflow.job_runs
WHERE job_name LIKE '%Tix Master ETL Pipeline%'
    AND start_time >= CURRENT_DATE - INTERVAL 30 DAYS

UNION ALL

SELECT 
    'Avg Duration (mins)' as metric,
    CAST(ROUND(AVG(TIMESTAMPDIFF(MINUTE, start_time, end_time)), 1) AS STRING) as value
FROM system.lakeflow.job_runs
WHERE job_name LIKE '%Tix Master ETL Pipeline%'
    AND start_time >= CURRENT_DATE - INTERVAL 30 DAYS
    AND result_state = 'SUCCESS';

-- ============================================================
-- USAGE INSTRUCTIONS
-- ============================================================
-- 
-- Quick Start:
-- 1. Run Query 1 to see today's costs
-- 2. Run Query 5 to see recent job runs
-- 3. Run Query 12 to check if you're over budget
-- 
-- For Dashboard:
-- 1. Create a Databricks SQL Dashboard
-- 2. Add visualizations using queries 16-17
-- 3. Add time-series chart using query 2 (7-day trend)
-- 4. Add pie chart using query 4 (cost by layer)
-- 5. Schedule dashboard refresh every hour
-- 
-- Automation:
-- 1. Create a scheduled job to run queries 14-15 daily
-- 2. Set up email alerts using query 12 (budget threshold)
-- 
-- ============================================================

