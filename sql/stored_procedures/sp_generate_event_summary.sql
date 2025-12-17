-- Stored Procedure: Generate Event Summary Reports
-- Demonstrates loops, cursors, conditional logic, and dynamic SQL

CREATE OR REPLACE PROCEDURE ticket_master.gold.sp_generate_event_summary(
  IN report_year INT,
  OUT total_reports INT,
  OUT execution_status STRING
)
LANGUAGE SQL
SQL SECURITY INVOKER
BEGIN
  -- Declare variables
  DECLARE month_num INT DEFAULT 1;
  DECLARE month_name STRING;
  DECLARE event_count INT;
  DECLARE venue_count INT;
  DECLARE attraction_count INT;
  DECLARE revenue_estimate DECIMAL(18,2);
  DECLARE avg_price DECIMAL(10,2);
  DECLARE top_venue STRING;
  DECLARE top_attraction STRING;
  DECLARE weekend_pct DECIMAL(5,2);
  DECLARE reports_generated INT DEFAULT 0;

  -- Error handler
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    SET execution_status = 'ERROR: ' || SQLERRM;
    SET total_reports = -1;
  END;

  -- Create summary table if not exists
  CREATE TABLE IF NOT EXISTS ticket_master.gold.monthly_event_summary (
    report_id BIGINT GENERATED ALWAYS AS IDENTITY,
    report_year INT,
    report_month INT,
    month_name STRING,
    total_events INT,
    total_venues INT,
    total_attractions INT,
    estimated_revenue DECIMAL(18,2),
    avg_ticket_price DECIMAL(10,2),
    top_venue STRING,
    top_attraction STRING,
    weekend_event_pct DECIMAL(5,2),
    created_at TIMESTAMP,
    CONSTRAINT monthly_summary_pk PRIMARY KEY (report_id)
  );

  -- Loop through each month
  WHILE month_num <= 12 DO
    -- Get month name
    SET month_name = (
      SELECT DISTINCT month_name
      FROM ticket_master.gold.dim_date
      WHERE year = report_year AND month = month_num
      LIMIT 1
    );

    -- Calculate monthly metrics
    SET event_count = (
      SELECT COUNT(DISTINCT f.event_id)
      FROM ticket_master.gold.fact_events f
      INNER JOIN ticket_master.gold.dim_date d ON f.event_date_key = d.date_key
      WHERE d.year = report_year AND d.month = month_num
    );
    
    SET venue_count = (
      SELECT COUNT(DISTINCT f.venue_sk)
      FROM ticket_master.gold.fact_events f
      INNER JOIN ticket_master.gold.dim_date d ON f.event_date_key = d.date_key
      WHERE d.year = report_year AND d.month = month_num
    );
    
    SET attraction_count = (
      SELECT COUNT(DISTINCT f.attraction_sk)
      FROM ticket_master.gold.fact_events f
      INNER JOIN ticket_master.gold.dim_date d ON f.event_date_key = d.date_key
      WHERE d.year = report_year AND d.month = month_num
    );
    
    SET revenue_estimate = (
      SELECT SUM(f.price_max)
      FROM ticket_master.gold.fact_events f
      INNER JOIN ticket_master.gold.dim_date d ON f.event_date_key = d.date_key
      WHERE d.year = report_year AND d.month = month_num
    );
    
    SET avg_price = (
      SELECT AVG(f.price_max)
      FROM ticket_master.gold.fact_events f
      INNER JOIN ticket_master.gold.dim_date d ON f.event_date_key = d.date_key
      WHERE d.year = report_year AND d.month = month_num
    );

    -- Only create report if there are events
    IF event_count > 0 THEN
      -- Find top venue for the month
      SET top_venue = (
        SELECT v.venue_name
        FROM ticket_master.gold.fact_events f
        INNER JOIN ticket_master.gold.dim_date d ON f.event_date_key = d.date_key
        INNER JOIN ticket_master.gold.dim_venue v ON f.venue_sk = v.venue_sk
        WHERE d.year = report_year AND d.month = month_num
        GROUP BY v.venue_name
        ORDER BY COUNT(*) DESC
        LIMIT 1
      );

      -- Find top attraction for the month
      SET top_attraction = (
        SELECT a.attraction_name
        FROM ticket_master.gold.fact_events f
        INNER JOIN ticket_master.gold.dim_date d ON f.event_date_key = d.date_key
        LEFT JOIN ticket_master.gold.dim_attraction a ON f.attraction_sk = a.attraction_sk
        WHERE d.year = report_year
          AND d.month = month_num
          AND a.attraction_name IS NOT NULL
        GROUP BY a.attraction_name
        ORDER BY COUNT(*) DESC
        LIMIT 1
      );

      -- Calculate weekend event percentage
      SET weekend_pct = (
        SELECT
          CAST(COUNT(CASE WHEN d.is_weekend THEN 1 END) * 100.0 / COUNT(*) AS DECIMAL(5,2))
        FROM ticket_master.gold.fact_events f
        INNER JOIN ticket_master.gold.dim_date d ON f.event_date_key = d.date_key
        WHERE d.year = report_year AND d.month = month_num
      );

      -- Insert summary record
      INSERT INTO ticket_master.gold.monthly_event_summary (
        report_year,
        report_month,
        month_name,
        total_events,
        total_venues,
        total_attractions,
        estimated_revenue,
        avg_ticket_price,
        top_venue,
        top_attraction,
        weekend_event_pct,
        created_at
      ) VALUES (
        report_year,
        month_num,
        month_name,
        event_count,
        venue_count,
        attraction_count,
        revenue_estimate,
        avg_price,
        top_venue,
        top_attraction,
        weekend_pct,
        CURRENT_TIMESTAMP()
      );

      SET reports_generated = reports_generated + 1;
    END IF;

    -- Move to next month
    SET month_num = month_num + 1;
  END WHILE;

  -- Set output parameters
  SET total_reports = reports_generated;

  IF reports_generated > 0 THEN
    SET execution_status = CONCAT(
      'SUCCESS: Generated ',
      reports_generated,
      ' monthly reports for ',
      report_year
    );
  ELSE
    SET execution_status = CONCAT(
      'WARNING: No events found for year ',
      report_year
    );
  END IF;

  -- Log execution
  INSERT INTO ticket_master.gold.etl_log (
    procedure_name,
    start_time,
    end_time,
    parameters,
    rows_processed,
    status
  ) VALUES (
    'sp_generate_event_summary',
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP(),
    CONCAT('report_year=', report_year),
    reports_generated,
    execution_status
  );

END;
