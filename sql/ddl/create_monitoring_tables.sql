-- ============================================================
-- Monitoring & Observability Tables
-- ============================================================

-- Table 1: Pipeline Cost Metrics
-- Tracks daily costs and resource consumption per job/task
CREATE TABLE IF NOT EXISTS ticket_master.gold.pipeline_cost_metrics (
  metric_id BIGINT GENERATED ALWAYS AS IDENTITY,
  run_date DATE NOT NULL,
  job_name STRING,
  task_name STRING,
  execution_duration_minutes INT,
  files_processed INT,
  rows_processed BIGINT,
  dbus_consumed DECIMAL(10,2),
  estimated_cost_usd DECIMAL(10,2),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT pipeline_cost_pk PRIMARY KEY (metric_id)
) 
COMMENT 'Daily pipeline cost and resource consumption metrics';

-- Table 2: Cost Alerts
-- Logs when costs exceed thresholds
CREATE TABLE IF NOT EXISTS ticket_master.gold.cost_alerts (
  alert_id BIGINT GENERATED ALWAYS AS IDENTITY,
  alert_date DATE NOT NULL,
  daily_cost DECIMAL(10,2),
  alert_type STRING,  -- 'HIGH_COST', 'SPIKE', 'BUDGET_WARNING'
  threshold_exceeded DECIMAL(10,2),
  message STRING,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT cost_alerts_pk PRIMARY KEY (alert_id)
) 
COMMENT 'Alerts when pipeline costs exceed defined thresholds';

-- Table 3: Stream Checkpoint Status
-- Tracks Auto Loader checkpoint metadata and lag
CREATE TABLE IF NOT EXISTS ticket_master.gold.stream_checkpoint_status (
  checkpoint_id BIGINT GENERATED ALWAYS AS IDENTITY,
  check_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  stream_name STRING NOT NULL,
  checkpoint_location STRING,
  last_checkpoint_time TIMESTAMP,
  files_pending INT,
  lag_minutes INT,
  status STRING,  -- 'HEALTHY', 'LAGGING', 'STALLED'
  CONSTRAINT stream_checkpoint_pk PRIMARY KEY (checkpoint_id)
) 
COMMENT 'Auto Loader checkpoint status and stream lag monitoring';

-- Table 4: Job Execution Metrics
-- Detailed metrics per job run
CREATE TABLE IF NOT EXISTS ticket_master.gold.job_execution_metrics (
  execution_id BIGINT GENERATED ALWAYS AS IDENTITY,
  job_id STRING NOT NULL,
  run_id STRING NOT NULL,
  job_name STRING,
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  duration_minutes INT,
  status STRING,  -- 'SUCCESS', 'FAILED', 'RUNNING', 'CANCELED'
  task_count INT,
  failed_tasks INT,
  total_dbus DECIMAL(10,2),
  total_cost_usd DECIMAL(10,2),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT job_execution_pk PRIMARY KEY (execution_id)
) 
COMMENT 'Detailed execution metrics for each job run';

-- Table 5: Layer Processing Stats
-- Tracks row counts and processing times by layer
CREATE TABLE IF NOT EXISTS ticket_master.gold.layer_processing_stats (
  stat_id BIGINT GENERATED ALWAYS AS IDENTITY,
  processing_date DATE NOT NULL,
  layer_name STRING NOT NULL,  -- 'BRONZE', 'SILVER', 'GOLD'
  table_name STRING NOT NULL,
  rows_inserted BIGINT,
  rows_updated BIGINT,
  rows_deleted BIGINT,
  processing_duration_seconds INT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT layer_stats_pk PRIMARY KEY (stat_id)
) 
COMMENT 'Processing statistics by layer and table';

-- ============================================================
-- Indexes for Query Performance
-- ============================================================

-- Note: Delta Lake automatically optimizes queries using liquid clustering
-- For tables with frequent date-based queries, consider adding:
-- ALTER TABLE ticket_master.gold.pipeline_cost_metrics 
--   CLUSTER BY (run_date, job_name);

-- ============================================================
-- Views for Easy Querying
-- ============================================================

-- View: Daily Cost Summary
CREATE OR REPLACE VIEW ticket_master.gold.vw_daily_cost_summary AS
SELECT 
  run_date,
  SUM(dbus_consumed) as total_dbus,
  SUM(estimated_cost_usd) as total_cost_usd,
  COUNT(DISTINCT job_name) as job_count,
  SUM(rows_processed) as total_rows_processed
FROM ticket_master.gold.pipeline_cost_metrics
GROUP BY run_date
ORDER BY run_date DESC;

-- View: Job Performance Trends
CREATE OR REPLACE VIEW ticket_master.gold.vw_job_performance_trends AS
SELECT 
  DATE(start_time) as run_date,
  job_name,
  COUNT(*) as run_count,
  AVG(duration_minutes) as avg_duration_min,
  MAX(duration_minutes) as max_duration_min,
  SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed_runs,
  AVG(total_cost_usd) as avg_cost_usd
FROM ticket_master.gold.job_execution_metrics
WHERE start_time >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY 1, 2
ORDER BY run_date DESC, job_name;

-- View: Stream Health Status
CREATE OR REPLACE VIEW ticket_master.gold.vw_stream_health AS
SELECT 
  stream_name,
  MAX(check_timestamp) as last_check,
  MAX(last_checkpoint_time) as last_checkpoint,
  TIMESTAMPDIFF(MINUTE, MAX(last_checkpoint_time), CURRENT_TIMESTAMP()) as lag_minutes,
  CASE 
    WHEN TIMESTAMPDIFF(MINUTE, MAX(last_checkpoint_time), CURRENT_TIMESTAMP()) < 60 THEN 'HEALTHY'
    WHEN TIMESTAMPDIFF(MINUTE, MAX(last_checkpoint_time), CURRENT_TIMESTAMP()) < 180 THEN 'LAGGING'
    ELSE 'STALLED'
  END as health_status
FROM ticket_master.gold.stream_checkpoint_status
GROUP BY stream_name
ORDER BY lag_minutes DESC;

-- ============================================================
-- Setup Complete!
-- ============================================================
-- Next Steps:
-- 1. Run queries from sql/queries/monitor_costs.sql to populate these tables
-- 2. Set up a scheduled job to refresh metrics daily
-- 3. Create a dashboard using these views
-- ============================================================

