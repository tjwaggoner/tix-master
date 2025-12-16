-- Create ETL Log Table for tracking procedure executions

CREATE TABLE IF NOT EXISTS ticketmaster.gold.etl_log (
  log_id BIGINT GENERATED ALWAYS AS IDENTITY,
  procedure_name STRING NOT NULL,
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  parameters STRING,
  rows_processed INT,
  status STRING,
  error_message STRING,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT etl_log_pk PRIMARY KEY (log_id)
);

-- Create indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_etl_log_procedure
  ON ticketmaster.gold.etl_log (procedure_name);

CREATE INDEX IF NOT EXISTS idx_etl_log_start_time
  ON ticketmaster.gold.etl_log (start_time);

-- Add table comment
COMMENT ON TABLE ticketmaster.gold.etl_log IS 'Tracks execution of ETL stored procedures including performance metrics and errors';
