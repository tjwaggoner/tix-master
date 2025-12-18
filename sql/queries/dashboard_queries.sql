-- ============================================================
-- Dashboard Query Definitions
-- Copy each query below into Databricks SQL to create visualizations
-- Fixed to use correct system table schemas
-- ============================================================

-- =============================================================================
-- QUERY 0: Hourly Cost Breakdown (Last 48 Hours)
-- =============================================================================
-- Name: Hourly Cost Trend
-- Visualization: Line Chart
-- X-Axis: usage_hour
-- Y-Axis: cost_usd
SELECT 
    DATE_TRUNC('HOUR', u.usage_start_time) as usage_hour,
    SUM(u.usage_quantity * p.pricing.default) as cost_usd,
    COUNT(DISTINCT u.usage_metadata.job_id) as job_count,
    SUM(u.usage_quantity) as total_dbus
FROM system.billing.usage u
INNER JOIN system.billing.list_prices p
    ON u.cloud = p.cloud
    AND u.sku_name = p.sku_name
    AND u.usage_start_time >= p.price_start_time
    AND (u.usage_end_time <= p.price_end_time OR p.price_end_time IS NULL)
WHERE u.usage_start_time >= CURRENT_TIMESTAMP - INTERVAL 48 HOURS
    AND u.billing_origin_product = 'JOBS'
    AND u.usage_metadata.job_id IS NOT NULL
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
SELECT 
    DATE_TRUNC('HOUR', u.usage_start_time) as usage_hour,
    COALESCE(u.usage_metadata.task_key, 'unknown') as task_name,
    SUM(u.usage_quantity * p.pricing.default) as cost_usd
FROM system.billing.usage u
INNER JOIN system.billing.list_prices p
    ON u.cloud = p.cloud
    AND u.sku_name = p.sku_name
    AND u.usage_start_time >= p.price_start_time
    AND (u.usage_end_time <= p.price_end_time OR p.price_end_time IS NULL)
WHERE u.usage_start_time >= CURRENT_TIMESTAMP - INTERVAL 24 HOURS
    AND u.billing_origin_product = 'JOBS'
    AND u.usage_metadata.job_id IS NOT NULL
GROUP BY 1, 2
ORDER BY usage_hour, cost_usd DESC;

-- =============================================================================
-- QUERY 0C: Current Hour Cost (Real-time Counter)
-- =============================================================================
-- Name: Current Hour Cost
-- Visualization: Counter
-- Format: Currency (2 decimals)
SELECT 
    COALESCE(SUM(u.usage_quantity * p.pricing.default), 0) as cost_usd
FROM system.billing.usage u
INNER JOIN system.billing.list_prices p
    ON u.cloud = p.cloud
    AND u.sku_name = p.sku_name
    AND u.usage_start_time >= p.price_start_time
    AND (u.usage_end_time <= p.price_end_time OR p.price_end_time IS NULL)
WHERE DATE_TRUNC('HOUR', u.usage_start_time) = DATE_TRUNC('HOUR', CURRENT_TIMESTAMP)
    AND u.billing_origin_product = 'JOBS'
    AND u.usage_metadata.job_id IS NOT NULL;

-- =============================================================================
-- QUERY 1: Cost Today (Counter visualization)
-- =============================================================================
-- Name: Cost Today
-- Visualization: Counter
-- Format: Currency (2 decimals)
SELECT 
    COALESCE(SUM(u.usage_quantity * p.pricing.default), 0) as cost_usd
FROM system.billing.usage u
INNER JOIN system.billing.list_prices p
    ON u.cloud = p.cloud
    AND u.sku_name = p.sku_name
    AND u.usage_start_time >= p.price_start_time
    AND (u.usage_end_time <= p.price_end_time OR p.price_end_time IS NULL)
WHERE DATE(u.usage_start_time) = CURRENT_DATE
    AND u.billing_origin_product = 'JOBS'
    AND u.usage_metadata.job_id IS NOT NULL;

-- =============================================================================
-- QUERY 2: Cost Yesterday (Counter visualization)
-- =============================================================================
-- Name: Cost Yesterday
-- Visualization: Counter
-- Format: Currency (2 decimals)
SELECT 
    COALESCE(SUM(u.usage_quantity * p.pricing.default), 0) as cost_usd
FROM system.billing.usage u
INNER JOIN system.billing.list_prices p
    ON u.cloud = p.cloud
    AND u.sku_name = p.sku_name
    AND u.usage_start_time >= p.price_start_time
    AND (u.usage_end_time <= p.price_end_time OR p.price_end_time IS NULL)
WHERE DATE(u.usage_start_time) = CURRENT_DATE - 1
    AND u.billing_origin_product = 'JOBS'
    AND u.usage_metadata.job_id IS NOT NULL;

-- =============================================================================
-- QUERY 3: Cost Last 7 Days (Counter visualization)
-- =============================================================================
-- Name: Cost 7 Days
-- Visualization: Counter
-- Format: Currency (2 decimals)
SELECT 
    COALESCE(SUM(u.usage_quantity * p.pricing.default), 0) as cost_usd
FROM system.billing.usage u
INNER JOIN system.billing.list_prices p
    ON u.cloud = p.cloud
    AND u.sku_name = p.sku_name
    AND u.usage_start_time >= p.price_start_time
    AND (u.usage_end_time <= p.price_end_time OR p.price_end_time IS NULL)
WHERE u.usage_start_time >= CURRENT_DATE - INTERVAL 7 DAYS
    AND u.billing_origin_product = 'JOBS'
    AND u.usage_metadata.job_id IS NOT NULL;

-- =============================================================================
-- QUERY 4: Cost Last 30 Days (Counter visualization)
-- =============================================================================
-- Name: Cost 30 Days
-- Visualization: Counter
-- Format: Currency (2 decimals)
SELECT 
    COALESCE(SUM(u.usage_quantity * p.pricing.default), 0) as cost_usd
FROM system.billing.usage u
INNER JOIN system.billing.list_prices p
    ON u.cloud = p.cloud
    AND u.sku_name = p.sku_name
    AND u.usage_start_time >= p.price_start_time
    AND (u.usage_end_time <= p.price_end_time OR p.price_end_time IS NULL)
WHERE u.usage_start_time >= CURRENT_DATE - INTERVAL 30 DAYS
    AND u.billing_origin_product = 'JOBS'
    AND u.usage_metadata.job_id IS NOT NULL;

-- =============================================================================
-- QUERY 5: 30-Day Cost Trend (Line Chart)
-- =============================================================================
-- Name: Cost Trend 30 Days
-- Visualization: Line Chart
-- X-Axis: usage_date
-- Y-Axis: cost_usd
SELECT 
    DATE(u.usage_start_time) as usage_date,
    SUM(u.usage_quantity * p.pricing.default) as cost_usd
FROM system.billing.usage u
INNER JOIN system.billing.list_prices p
    ON u.cloud = p.cloud
    AND u.sku_name = p.sku_name
    AND u.usage_start_time >= p.price_start_time
    AND (u.usage_end_time <= p.price_end_time OR p.price_end_time IS NULL)
WHERE u.usage_start_time >= CURRENT_DATE - INTERVAL 30 DAYS
    AND u.billing_origin_product = 'JOBS'
    AND u.usage_metadata.job_id IS NOT NULL
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
        WHEN u.usage_metadata.task_key LIKE '%bronze%' THEN 'BRONZE'
        WHEN u.usage_metadata.task_key LIKE '%silver%' THEN 'SILVER'
        WHEN u.usage_metadata.task_key LIKE '%gold%' THEN 'GOLD'
        WHEN u.usage_metadata.task_key LIKE '%ingest%' THEN 'INGESTION'
        ELSE 'OTHER'
    END as layer,
    SUM(u.usage_quantity * p.pricing.default) as cost_usd
FROM system.billing.usage u
INNER JOIN system.billing.list_prices p
    ON u.cloud = p.cloud
    AND u.sku_name = p.sku_name
    AND u.usage_start_time >= p.price_start_time
    AND (u.usage_end_time <= p.price_end_time OR p.price_end_time IS NULL)
WHERE u.usage_start_time >= CURRENT_DATE - INTERVAL 7 DAYS
    AND u.billing_origin_product = 'JOBS'
    AND u.usage_metadata.job_id IS NOT NULL
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
WHERE start_time >= CURRENT_DATE - INTERVAL 30 DAYS
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
WHERE start_time >= CURRENT_DATE - INTERVAL 30 DAYS
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
        DATE(u.usage_start_time) as usage_date,
        SUM(u.usage_quantity * p.pricing.default) as daily_cost
    FROM system.billing.usage u
    INNER JOIN system.billing.list_prices p
        ON u.cloud = p.cloud
        AND u.sku_name = p.sku_name
        AND u.usage_start_time >= p.price_start_time
        AND (u.usage_end_time <= p.price_end_time OR p.price_end_time IS NULL)
    WHERE u.usage_start_time >= CURRENT_DATE - INTERVAL 7 DAYS
        AND u.billing_origin_product = 'JOBS'
        AND u.usage_metadata.job_id IS NOT NULL
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
    t.table_schema as layer,
    t.table_name,
    ROUND(COALESCE(ts.total_size_bytes, 0) / 1024 / 1024 / 1024, 2) as size_gb
FROM system.information_schema.tables t
LEFT JOIN system.information_schema.table_storage_stats ts 
    ON t.table_catalog = ts.table_catalog 
    AND t.table_schema = ts.table_schema 
    AND t.table_name = ts.table_name
WHERE t.table_catalog = 'ticket_master'
    AND t.table_schema IN ('bronze', 'silver', 'gold')
    AND t.table_type = 'MANAGED'
ORDER BY size_gb DESC
LIMIT 20;

-- =============================================================================
-- QUERY 11: Performance Summary (Table/Counters)
-- =============================================================================
-- Name: Performance Summary
-- Visualization: Counters or Table
SELECT 
    'Total Runs (30d)' as metric,
    CAST(COUNT(*) AS STRING) as value
FROM system.lakeflow.job_runs
WHERE start_time >= CURRENT_DATE - INTERVAL 30 DAYS

UNION ALL

SELECT 
    'Success Rate (30d)' as metric,
    CONCAT(CAST(ROUND(100.0 * SUM(CASE WHEN result_state = 'SUCCESS' THEN 1 ELSE 0 END) / COUNT(*), 1) AS STRING), '%') as value
FROM system.lakeflow.job_runs
WHERE start_time >= CURRENT_DATE - INTERVAL 30 DAYS

UNION ALL

SELECT 
    'Avg Duration (mins)' as metric,
    CAST(ROUND(AVG(TIMESTAMPDIFF(MINUTE, start_time, end_time)), 1) AS STRING) as value
FROM system.lakeflow.job_runs
WHERE start_time >= CURRENT_DATE - INTERVAL 30 DAYS
    AND result_state = 'SUCCESS';

-- ============================================================
-- NOTES ON SCHEMA FIXES
-- ============================================================
-- 
-- Fixed issues:
-- 1. Replaced `list_price` with proper join to `system.billing.list_prices`
-- 2. Added `u.billing_origin_product = 'JOBS'` filter for job-related costs
-- 3. Changed `::STRING` to `CAST(... AS STRING)` for proper SQL syntax
-- 4. Removed `usage_metadata.job_name` filter (use job_id instead)
-- 5. Added proper NULL checks for usage_metadata fields
-- 6. Fixed table_storage_stats aggregation (no SUM needed, already aggregated)
-- 
-- ============================================================
