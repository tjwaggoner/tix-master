-- ============================================================
-- Dashboard Query Definitions
-- Copy each query below into Databricks SQL to create visualizations
-- Fixed to use correct system table schemas
-- ============================================================

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
    COALESCE(u.usage_metadata.job_name, 'unknown') as job_name,
    SUM(u.usage_quantity * p.pricing.`default`) as cost_usd
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
    AND u.usage_metadata.job_id IS NOT NULL
    AND u.workspace_id = '7474655348825387'

-- =============================================================================
-- QUERY 3: Cost Last 7 days by job type
-- =============================================================================
-- Name: L7D Cost Today
-- Visualization: Counter
-- Format: Currency (2 decimals)


    SELECT 
    DATE(u.usage_start_time) as usage_date,
    COALESCE(u.usage_metadata.job_name, 'unknown') as job_name,
    SUM(u.usage_quantity * p.pricing.`default`) as cost_usd
FROM system.billing.usage u
INNER JOIN system.billing.list_prices p
    ON u.cloud = p.cloud
    AND u.sku_name = p.sku_name
    AND u.usage_start_time >= p.price_start_time
    AND (u.usage_end_time <= p.price_end_time OR p.price_end_time IS NULL)
WHERE DATE(u.usage_start_time) >= CURRENT_DATE - INTERVAL 7 DAYS
    AND u.billing_origin_product = 'JOBS'
    AND u.usage_metadata.job_id IS NOT NULL
    AND u.workspace_id = '7474655348825387'
GROUP BY 1, 2
ORDER BY usage_date DESC, cost_usd DESC;


