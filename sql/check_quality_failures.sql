-- Check for Data Quality Failures
-- This query fails the task if any data quality checks failed in the latest run

-- Get the latest check run timestamp
DECLARE latest_run TIMESTAMP;
SET latest_run = (
  SELECT MAX(check_run_timestamp) 
  FROM ticket_master.gold.data_quality_results
);

-- Check for failures in the latest run
DECLARE failure_count INT;
SET failure_count = (
  SELECT COUNT(*) 
  FROM ticket_master.gold.data_quality_results
  WHERE check_run_timestamp = latest_run
    AND check_status = 'FAILED'
);

-- Display results
SELECT 
  latest_run AS check_run_timestamp,
  failure_count AS failed_checks,
  CASE 
    WHEN failure_count > 0 THEN 'ALERT: Data Quality Failures Detected!'
    ELSE 'SUCCESS: All Quality Checks Passed'
  END AS alert_status
FROM (SELECT 1);

-- Raise error if failures detected (this will fail the task and trigger email)
IF failure_count > 0 THEN
  SIGNAL SQLSTATE '45000' 
    SET MESSAGE_TEXT = CONCAT('Data Quality Alert: ', failure_count, ' check(s) failed in the latest run. Check ticket_master.gold.data_quality_results for details.');
END IF;

