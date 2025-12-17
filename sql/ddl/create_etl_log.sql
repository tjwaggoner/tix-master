-- Create ETL Log Table for tracking procedure executions

CREATE TABLE IF NOT EXISTS ticket_master.gold.etl_log (
  log_id BIGINT GENERATED ALWAYS AS IDENTITY,
  procedure_name STRING NOT NULL,
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  parameters STRING,
  rows_processed INT,
  status STRING,
  error_message STRING,
  created_at TIMESTAMP,
  CONSTRAINT etl_log_pk PRIMARY KEY (log_id)
);

-- Add table comment
COMMENT ON TABLE ticket_master.gold.etl_log IS 'Tracks execution of ETL stored procedures including performance metrics and errors';
