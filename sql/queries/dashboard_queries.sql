-- ============================================================
-- Dashboard Query Definitions
-- Copy each query below into Databricks SQL to create visualizations
-- ============================================================

-- =============================================================================
-- QUERY 0: Hourly Cost Breakdown (Last 48 Hours)
-- =============================================================================
-- Name: Hourly Cost Trend
-- Visualization: Line Chart
-- X-Axis: usage_hour
-- Y-Axis: cost_usd
-- Description: Shows cost by hour for the last 48 hours to identify usage patterns
SELECT 
    DATE_TRUNC('HOUR', usage_start_time) as usage_hour,
    SUM(usage_quantity * list_price) as cost_usd,
    COUNT(DISTINCT usage_metadata.job_id) as job_count,
    SUM(usage_quantity) as total_dbus
FROM system.billing.usage
WHERE usage_start_time >= CURRENT_TIMESTAMP - INTERVAL 48 HOURS
    AND usage_metadata.job_name LIKE '%Tix Master%'
GROUP BY 1
ORDER BY usage_hour;

-- =============================================================================
-- QUERY 0B: Hourly Cost by Task (Last 24 Hours)
-- =============================================================================
-- Name: Hourly Cost by Task
-- Visualization: Stacked Area Chart
-- X-Axis: usage_hour
-- Y-Axis: cost_usd
-- Group by: task_name
-- Description: Shows which tasks are most expensive by hour
SELECT 
    DATE_TRUNC('HOUR', usage_start_time) as usage_hour,
    COALESCE(usage_metadata.task_key, 'unknown') as task_name,
    SUM(usage_quantity * list_price) as cost_usd
FROM system.billing.usage
WHERE usage_start_time >= CURRENT_TIMESTAMP - INTERVAL 24 HOURS
    AND usage_metadata.job_name LIKE '%Tix Master%'
GROUP BY 1, 2
ORDER BY usage_hour, cost_usd DESC;

-- =============================================================================
-- QUERY 0C: Current Hour Cost (Real-time Counter)
-- =============================================================================
-- Name: Current Hour Cost
-- Visualization: Counter
-- Format: Currency (2 decimals)
-- Description: Shows cost accumulating in the current hour
SELECT 
    COALESCE(SUM(usage_quantity * list_price), 0) as cost_usd
FROM system.billing.usage
WHERE DATE_TRUNC('HOUR', usage_start_time) = DATE_TRUNC('HOUR', CURRENT_TIMESTAMP)
    AND usage_metadata.job_name LIKE '%Tix Master%';

-- =============================================================================
-- QUERY 1: Cost Today (Counter visualization)
-- =============================================================================
-- Name: Cost Today
-- Visualization: Counter
-- Format: Currency (2 decimals)
SELECT 
    COALESCE(SUM(usage_quantity * list_price), 0) as cost_usd
FROM system.billing.usage
WHERE DATE(usage_start_time) = CURRENT_DATE
    AND usage_metadata.job_name LIKE '%Tix Master%';

-- =============================================================================
-- QUERY 2: Cost Yesterday (Counter visualization)
-- =============================================================================
-- Name: Cost Yesterday
-- Visualization: Counter
-- Format: Currency (2 decimals)
SELECT 
    COALESCE(SUM(usage_quantity * list_price), 0) as cost_usd
FROM system.billing.usage
WHERE DATE(usage_start_time) = CURRENT_DATE - 1
    AND usage_metadata.job_name LIKE '%Tix Master%';

-- =============================================================================
-- QUERY 3: Cost Last 7 Days (Counter visualization)
-- =============================================================================
-- Name: Cost 7 Days
-- Visualization: Counter
-- Format: Currency (2 decimals)
SELECT 
    COALESCE(SUM(usage_quantity * list_price), 0) as cost_usd
FROM system.billing.usage
WHERE usage_start_time >= CURRENT_DATE - INTERVAL 7 DAYS
    AND usage_metadata.job_name LIKE '%Tix Master%';

-- =============================================================================
-- QUERY 4: Cost Last 30 Days (Counter visualization)
-- =============================================================================
-- Name: Cost 30 Days
-- Visualization: Counter
-- Format: Currency (2 decimals)
SELECT 
    COALESCE(SUM(usage_quantity * list_price), 0) as cost_usd
FROM system.billing.usage
WHERE usage_start_time >= CURRENT_DATE - INTERVAL 30 DAYS
    AND usage_metadata.job_name LIKE '%Tix Master%';

-- =============================================================================
-- QUERY 5: 30-Day Cost Trend (Line Chart)
-- =============================================================================
-- Name: Cost Trend 30 Days
-- Visualization: Line Chart
-- X-Axis: usage_date
-- Y-Axis: cost_usd
SELECT 
    DATE(usage_start_time) as usage_date,
    SUM(usage_quantity * list_price) as cost_usd
FROM system.billing.usage
WHERE usage_start_time >= CURRENT_DATE - INTERVAL 30 DAYS
    AND usage_metadata.job_name LIKE '%Tix Master%'
GROUP BY 1
ORDER BY usage_date;

-- =============================================================================
-- QUERY 6: Cost by Layer (Pie Chart)
-- =============================================================================
-- Name: Cost by Layer
-- Visualization: Pie Chart
-- Group by: layer
-- Values: cost_usd
SELECT 
    CASE 
        WHEN usage_metadata.task_key LIKE '%bronze%' THEN 'BRONZE'
        WHEN usage_metadata.task_key LIKE '%silver%' THEN 'SILVER'
        WHEN usage_metadata.task_key LIKE '%gold%' THEN 'GOLD'
        WHEN usage_metadata.task_key LIKE '%ingest%' THEN 'INGESTION'
        ELSE 'OTHER'
    END as layer,
    SUM(usage_quantity * list_price) as cost_usd
FROM system.billing.usage
WHERE usage_start_time >= CURRENT_DATE - INTERVAL 7 DAYS
    AND usage_metadata.job_name LIKE '%Tix Master%'
GROUP BY 1
ORDER BY cost_usd DESC;

-- =============================================================================
-- QUERY 7: Job Success Rate (Stacked Bar Chart)
-- =============================================================================
-- Name: Job Success Rate
-- Visualization: Stacked Bar Chart
-- X-Axis: run_date
-- Y-Axis: successful_runs (green), failed_runs (red)
SELECT 
    DATE(start_time) as run_date,
    SUM(CASE WHEN result_state = 'SUCCESS' THEN 1 ELSE 0 END) as successful_runs,
    SUM(CASE WHEN result_state = 'FAILED' THEN 1 ELSE 0 END) as failed_runs,
    ROUND(100.0 * SUM(CASE WHEN result_state = 'SUCCESS' THEN 1 ELSE 0 END) / COUNT(*), 2) as success_rate_pct
FROM system.lakeflow.job_runs
WHERE job_name LIKE '%Tix Master ETL Pipeline%'
    AND start_time >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY 1
ORDER BY run_date;

-- =============================================================================
-- QUERY 8: Job Duration Trend (Line Chart with Min/Max)
-- =============================================================================
-- Name: Job Duration Trend
-- Visualization: Line Chart (with range shading)
-- X-Axis: run_date
-- Y-Axis: avg_duration_minutes
SELECT 
    DATE(start_time) as run_date,
    AVG(TIMESTAMPDIFF(MINUTE, start_time, end_time)) as avg_duration_minutes,
    MIN(TIMESTAMPDIFF(MINUTE, start_time, end_time)) as min_duration_minutes,
    MAX(TIMESTAMPDIFF(MINUTE, start_time, end_time)) as max_duration_minutes
FROM system.lakeflow.job_runs
WHERE job_name LIKE '%Tix Master ETL Pipeline%'
    AND start_time >= CURRENT_DATE - INTERVAL 30 DAYS
    AND result_state = 'SUCCESS'
GROUP BY 1
ORDER BY run_date;

-- =============================================================================
-- QUERY 9: Budget Status (Table with conditional formatting)
-- =============================================================================
-- Name: Budget Status
-- Visualization: Table
-- Conditional Formatting: Color code 'status' column
SELECT 
    usage_date,
    ROUND(daily_cost, 2) as daily_cost,
    50.00 as budget_threshold,
    CASE 
        WHEN daily_cost > 50.00 THEN '⚠️ OVER BUDGET'
        WHEN daily_cost > 40.00 THEN '⚡ WARNING'
        ELSE '✅ OK'
    END as status
FROM (
    SELECT 
        DATE(usage_start_time) as usage_date,
        SUM(usage_quantity * list_price) as daily_cost
    FROM system.billing.usage
    WHERE usage_metadata.job_name LIKE '%Tix Master%'
        AND usage_start_time >= CURRENT_DATE - INTERVAL 7 DAYS
    GROUP BY 1
) daily_costs
ORDER BY usage_date DESC;

-- =============================================================================
-- QUERY 10: Table Sizes (Bar Chart)
-- =============================================================================
-- Name: Table Sizes by Layer
-- Visualization: Bar Chart
-- X-Axis: table_name
-- Y-Axis: size_gb
-- Group by: layer
SELECT 
    table_schema as layer,
    table_name,
    ROUND(SUM(total_size_bytes) / 1024 / 1024 / 1024, 2) as size_gb
FROM system.information_schema.tables t
LEFT JOIN system.information_schema.table_storage_stats ts 
    ON t.table_catalog = ts.table_catalog 
    AND t.table_schema = ts.table_schema 
    AND t.table_name = ts.table_name
WHERE t.table_catalog = 'ticket_master'
    AND t.table_schema IN ('bronze', 'silver', 'gold')
    AND t.table_type = 'MANAGED'
GROUP BY 1, 2
ORDER BY size_gb DESC
LIMIT 20;

-- =============================================================================
-- QUERY 11: Performance Summary (Table/Counters)
-- =============================================================================
-- Name: Performance Summary
-- Visualization: Counters or Table
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
-- HOW TO USE THESE QUERIES
-- ============================================================
-- 
-- 1. Navigate to SQL Warehouses in Databricks
-- 2. Click "Queries" in the left sidebar
-- 3. Create a new query for each section above
-- 4. Copy the SQL and save with the specified name
-- 5. Add the recommended visualization type
-- 6. Create a new dashboard and add all query visualizations
-- 7. Arrange them using the layout guide in DASHBOARD_SETUP.md
-- 
-- ============================================================

