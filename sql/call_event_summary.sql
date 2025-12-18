-- Call Event Summary Generation Stored Procedure
DECLARE total_reports INT;
DECLARE execution_status STRING;

CALL ticket_master.gold.sp_generate_event_summary(
  2025,
  total_reports,
  execution_status
);

SELECT total_reports, execution_status;

