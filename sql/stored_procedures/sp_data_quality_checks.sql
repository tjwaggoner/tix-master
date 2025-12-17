-- Stored Procedure: Data Quality Checks
-- Demonstrates IF/ELSE logic, CASE statements, and validation rules

CREATE OR REPLACE PROCEDURE ticket_master.gold.sp_data_quality_checks(
  OUT total_checks INT,
  OUT failed_checks INT,
  OUT quality_score DECIMAL(5,2),
  OUT execution_status STRING
)
LANGUAGE SQL
SQL SECURITY INVOKER
BEGIN
  -- Declare variables
  DECLARE check_count INT DEFAULT 0;
  DECLARE failure_count INT DEFAULT 0;
  DECLARE check_name STRING;
  DECLARE check_result STRING;
  DECLARE orphaned_count INT;
  DECLARE null_pk_count INT;
  DECLARE duplicate_count INT;
  DECLARE run_timestamp TIMESTAMP;
  DECLARE invalid_dates INT;
  DECLARE invalid_prices INT;
  DECLARE missing_venues INT;
  DECLARE bronze_count INT;
  DECLARE silver_count INT;
  DECLARE count_diff INT;

  -- Error handler
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    SET execution_status = 'ERROR: ' || SQLERRM;
    SET total_checks = -1;
    SET failed_checks = -1;
    SET quality_score = 0.0;
  END;

  -- Create quality check results table if not exists
  CREATE TABLE IF NOT EXISTS ticket_master.gold.data_quality_results (
    check_id BIGINT GENERATED ALWAYS AS IDENTITY,
    check_run_timestamp TIMESTAMP,
    check_name STRING,
    check_status STRING,
    records_affected INT,
    severity STRING,
    details STRING,
    CONSTRAINT dq_results_pk PRIMARY KEY (check_id)
  );

  -- Start quality check run
  SET run_timestamp = CURRENT_TIMESTAMP();

  -- CHECK 1: Orphaned event-venue relationships
  SET check_count = check_count + 1;
  SET check_name = 'Orphaned Event-Venue Records';

  SET orphaned_count = (
    SELECT COUNT(*)
    FROM ticket_master.silver.event_venues ev
    LEFT JOIN ticket_master.silver.events e ON ev.event_id = e.event_id
    WHERE e.event_id IS NULL
  );

  IF orphaned_count > 0 THEN
    SET failure_count = failure_count + 1;
    SET check_result = 'FAILED';
  ELSE
    SET check_result = 'PASSED';
  END IF;

  INSERT INTO ticket_master.gold.data_quality_results
    (check_run_timestamp, check_name, check_status, records_affected, severity, details)
  VALUES
    (run_timestamp, check_name, check_result, orphaned_count, 'HIGH',
     CONCAT('Found ', orphaned_count, ' orphaned event-venue relationships'));

  -- CHECK 2: NULL primary keys in fact table
  SET check_count = check_count + 1;
  SET check_name = 'NULL Primary Keys in Fact Events';

  SET null_pk_count = (
    SELECT COUNT(*)
    FROM ticket_master.gold.fact_events
    WHERE event_id IS NULL
  );

  IF null_pk_count > 0 THEN
    SET failure_count = failure_count + 1;
    SET check_result = 'FAILED';
  ELSE
    SET check_result = 'PASSED';
  END IF;

  INSERT INTO ticket_master.gold.data_quality_results
    (check_run_timestamp, check_name, check_status, records_affected, severity, details)
  VALUES
    (run_timestamp, check_name, check_result, null_pk_count, 'CRITICAL',
     CONCAT('Found ', null_pk_count, ' NULL event_ids in fact table'));

  -- CHECK 3: Duplicate events in fact table
  SET check_count = check_count + 1;
  SET check_name = 'Duplicate Events';

  SET duplicate_count = (
    SELECT COUNT(*)
    FROM (
      SELECT event_id, COUNT(*) as cnt
      FROM ticket_master.gold.fact_events
      GROUP BY event_id
      HAVING cnt > 1
    )
  );

  IF duplicate_count > 0 THEN
    SET failure_count = failure_count + 1;
    SET check_result = 'FAILED';
  ELSE
    SET check_result = 'PASSED';
  END IF;

  INSERT INTO ticket_master.gold.data_quality_results
    (check_run_timestamp, check_name, check_status, records_affected, severity, details)
  VALUES
    (run_timestamp, check_name, check_result, duplicate_count, 'HIGH',
     CONCAT('Found ', duplicate_count, ' duplicate events'));

  -- CHECK 4: Events with invalid dates
  SET check_count = check_count + 1;
  SET check_name = 'Invalid Event Dates';

  SET invalid_dates = (
    SELECT COUNT(*)
    FROM ticket_master.gold.fact_events
    WHERE event_date_key NOT IN (
      SELECT date_key FROM ticket_master.gold.dim_date
    )
  );

  IF invalid_dates > 0 THEN
    SET failure_count = failure_count + 1;
    SET check_result = 'FAILED';
  ELSE
    SET check_result = 'PASSED';
  END IF;

  INSERT INTO ticket_master.gold.data_quality_results
    (check_run_timestamp, check_name, check_status, records_affected, severity, details)
  VALUES
    (run_timestamp, check_name, check_result, invalid_dates, 'MEDIUM',
     CONCAT('Found ', invalid_dates, ' events with invalid date keys'));

  -- CHECK 5: Price validation
  SET check_count = check_count + 1;
  SET check_name = 'Invalid Price Ranges';

  SET invalid_prices = (
    SELECT COUNT(*)
    FROM ticket_master.gold.fact_events
    WHERE price_min > price_max
       OR price_min < 0
       OR price_max < 0
  );

  IF invalid_prices > 0 THEN
    SET failure_count = failure_count + 1;
    SET check_result = 'FAILED';
  ELSE
    SET check_result = 'PASSED';
  END IF;

  INSERT INTO ticket_master.gold.data_quality_results
    (check_run_timestamp, check_name, check_status, records_affected, severity, details)
  VALUES
    (run_timestamp, check_name, check_result, invalid_prices, 'MEDIUM',
     CONCAT('Found ', invalid_prices, ' events with invalid prices'));

  -- CHECK 6: Referential integrity - venues
  SET check_count = check_count + 1;
  SET check_name = 'Missing Venue References';

  SET missing_venues = (
    SELECT COUNT(*)
    FROM ticket_master.gold.fact_events
    WHERE venue_sk IS NOT NULL
      AND venue_sk NOT IN (SELECT venue_sk FROM ticket_master.gold.dim_venue)
  );

  IF missing_venues > 0 THEN
    SET failure_count = failure_count + 1;
    SET check_result = 'FAILED';
  ELSE
    SET check_result = 'PASSED';
  END IF;

  INSERT INTO ticket_master.gold.data_quality_results
    (check_run_timestamp, check_name, check_status, records_affected, severity, details)
  VALUES
    (run_timestamp, check_name, check_result, missing_venues, 'HIGH',
     CONCAT('Found ', missing_venues, ' events with invalid venue references'));

  -- CHECK 7: Record counts consistency
  SET check_count = check_count + 1;
  SET check_name = 'Bronze-Silver Record Count Match';

  SET bronze_count = (SELECT COUNT(*) FROM ticket_master.bronze.events_raw);
  SET silver_count = (SELECT COUNT(*) FROM ticket_master.silver.events);
  SET count_diff = ABS(bronze_count - silver_count);

  -- Allow up to 5% difference for deduplication
  IF count_diff > (bronze_count * 0.05) THEN
    SET failure_count = failure_count + 1;
    SET check_result = 'WARNING';
  ELSE
    SET check_result = 'PASSED';
  END IF;

  INSERT INTO ticket_master.gold.data_quality_results
    (check_run_timestamp, check_name, check_status, records_affected, severity, details)
  VALUES
    (run_timestamp, check_name, check_result, count_diff, 'LOW',
     CONCAT('Bronze: ', bronze_count, ', Silver: ', silver_count, ', Diff: ', count_diff));

  -- Calculate quality score
  SET total_checks = check_count;
  SET failed_checks = failure_count;

  IF check_count > 0 THEN
    SET quality_score = ((check_count - failure_count) * 100.0) / check_count;
  ELSE
    SET quality_score = 100.0;
  END IF;

  -- Determine overall status
  IF failure_count = 0 THEN
    SET execution_status = 'SUCCESS: All data quality checks passed';
  ELSE
    SET execution_status = CONCAT(
      'WARNING: ',
      failure_count,
      ' out of ',
      check_count,
      ' checks failed. Quality score: ',
      ROUND(quality_score, 2),
      '%'
    );
  END IF;

  -- Log execution
  INSERT INTO ticket_master.gold.etl_log (
    procedure_name,
    start_time,
    end_time,
    rows_processed,
    status
  ) VALUES (
    'sp_data_quality_checks',
    run_timestamp,
    CURRENT_TIMESTAMP(),
    check_count,
    execution_status
  );

END;
