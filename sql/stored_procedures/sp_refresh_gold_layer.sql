-- Stored Procedure: Refresh Gold Layer
-- Demonstrates control flow, loops, error handling, and identity key usage

CREATE OR REPLACE PROCEDURE ticketmaster.gold.sp_refresh_gold_layer(
  IN start_date DATE,
  IN end_date DATE,
  OUT rows_processed INT,
  OUT execution_status STRING
)
LANGUAGE SQL
BEGIN
  -- Declare variables
  DECLARE current_date DATE;
  DECLARE venue_count INT DEFAULT 0;
  DECLARE attraction_count INT DEFAULT 0;
  DECLARE event_count INT DEFAULT 0;
  DECLARE error_message STRING DEFAULT '';
  DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
  BEGIN
    SET execution_status = 'ERROR: ' || SQLERRM;
    SET rows_processed = -1;
  END;

  -- Log start
  INSERT INTO ticketmaster.gold.etl_log (
    procedure_name,
    start_time,
    parameters,
    status
  ) VALUES (
    'sp_refresh_gold_layer',
    CURRENT_TIMESTAMP(),
    CONCAT('start_date=', start_date, ', end_date=', end_date),
    'RUNNING'
  );

  -- Validate date range
  IF start_date > end_date THEN
    SET execution_status = 'ERROR: start_date cannot be after end_date';
    SET rows_processed = -1;
    RETURN;
  END IF;

  -- Step 1: Refresh Dimension Tables
  -- Refresh dim_venue (SCD Type 2)
  MERGE INTO ticketmaster.gold.dim_venue AS target
  USING (
    SELECT
      venue_id,
      venue_name,
      venue_type,
      city,
      state,
      state_code,
      country,
      country_code,
      postal_code,
      address_line1,
      latitude,
      longitude,
      timezone,
      venue_url,
      _ingestion_timestamp
    FROM ticketmaster.silver.venues
    WHERE _ingestion_timestamp BETWEEN start_date AND end_date
  ) AS source
  ON target.venue_id = source.venue_id AND target.is_current = TRUE
  WHEN MATCHED AND (
    target.venue_name != source.venue_name OR
    target.city != source.city OR
    target.state != source.state
  ) THEN
    UPDATE SET
      valid_to = CURRENT_TIMESTAMP(),
      is_current = FALSE
  WHEN NOT MATCHED THEN
    INSERT (
      venue_id, venue_name, venue_type, city, state, state_code,
      country, country_code, postal_code, address_line1,
      latitude, longitude, timezone, venue_url,
      valid_from, valid_to, is_current
    ) VALUES (
      source.venue_id, source.venue_name, source.venue_type,
      source.city, source.state, source.state_code,
      source.country, source.country_code, source.postal_code,
      source.address_line1, source.latitude, source.longitude,
      source.timezone, source.venue_url,
      source._ingestion_timestamp, NULL, TRUE
    );

  SET venue_count = (SELECT COUNT(*) FROM ticketmaster.gold.dim_venue WHERE is_current = TRUE);

  -- Step 2: Refresh Fact Table
  -- Insert new events that don't exist in fact table
  INSERT INTO ticketmaster.gold.fact_events
  SELECT
    DEFAULT as event_sk,  -- Use identity column
    e.event_id,
    e.event_name,
    e.event_type,
    CAST(date_format(e.event_date, 'yyyyMMdd') AS INT) as event_date_key,
    dv.venue_sk,
    da.attraction_sk,
    NULL as classification_sk,
    e.event_datetime,
    e.event_time,
    e.event_timezone,
    e.price_min,
    e.price_max,
    e.price_currency,
    e.status_code,
    e.sales_start_datetime,
    e.sales_end_datetime,
    e.is_test,
    e.event_url
  FROM ticketmaster.silver.events e
  LEFT JOIN ticketmaster.silver.event_venues ev ON e.event_id = ev.event_id
  LEFT JOIN ticketmaster.gold.dim_venue dv ON ev.venue_id = dv.venue_id AND dv.is_current = TRUE
  LEFT JOIN ticketmaster.silver.event_attractions ea ON e.event_id = ea.event_id
  LEFT JOIN ticketmaster.gold.dim_attraction da ON ea.attraction_id = da.attraction_id AND da.is_current = TRUE
  WHERE e.event_date BETWEEN start_date AND end_date
    AND NOT EXISTS (
      SELECT 1 FROM ticketmaster.gold.fact_events f
      WHERE f.event_id = e.event_id
    );

  SET event_count = (SELECT COUNT(*) FROM ticketmaster.gold.fact_events);

  -- Step 3: Optimize tables
  OPTIMIZE ticketmaster.gold.fact_events;
  OPTIMIZE ticketmaster.gold.dim_venue;
  OPTIMIZE ticketmaster.gold.dim_attraction;

  -- Step 4: Update statistics
  ANALYZE TABLE ticketmaster.gold.fact_events COMPUTE STATISTICS FOR ALL COLUMNS;
  ANALYZE TABLE ticketmaster.gold.dim_venue COMPUTE STATISTICS FOR ALL COLUMNS;

  -- Calculate total rows processed
  SET rows_processed = venue_count + attraction_count + event_count;
  SET execution_status = CONCAT(
    'SUCCESS: Processed ',
    venue_count, ' venues, ',
    attraction_count, ' attractions, ',
    event_count, ' events'
  );

  -- Log completion
  INSERT INTO ticketmaster.gold.etl_log (
    procedure_name,
    end_time,
    rows_processed,
    status
  ) VALUES (
    'sp_refresh_gold_layer',
    CURRENT_TIMESTAMP(),
    rows_processed,
    execution_status
  );

END;
