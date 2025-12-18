-- Run Data Quality Checks Procedure

DECLARE total_checks INT;
DECLARE failed_checks INT;
DECLARE quality_score DECIMAL(5,2);
DECLARE execution_status STRING;

CALL ticket_master.gold.sp_data_quality_checks(
  total_checks, 
  failed_checks, 
  quality_score, 
  execution_status
);

SELECT 
  total_checks,
  failed_checks, 
  quality_score,
  execution_status;

