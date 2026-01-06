-- ============================================================
-- Drop All Tix-Master Pipeline Resources
-- ============================================================
-- WARNING: This will delete ALL data from the pipeline!
-- Use this script carefully, typically only in dev/test environments.
-- ============================================================

USE CATALOG ticket_master;

-- ============================================================
-- Drop Gold Layer Tables (Star Schema)
-- ============================================================
-- Drop fact table first (has FKs to dimensions)
DROP TABLE IF EXISTS ticket_master.gold.fact_events;

-- Drop dimension tables
DROP TABLE IF EXISTS ticket_master.gold.dim_venue;
DROP TABLE IF EXISTS ticket_master.gold.dim_attraction;
DROP TABLE IF EXISTS ticket_master.gold.dim_date;
DROP TABLE IF EXISTS ticket_master.gold.dim_classification;
DROP TABLE IF EXISTS ticket_master.gold.dim_market;

-- Drop tables created by stored procedures
DROP TABLE IF EXISTS ticket_master.gold.data_quality_results;
DROP TABLE IF EXISTS ticket_master.gold.monthly_event_summary;

-- Drop ETL log table
DROP TABLE IF EXISTS ticket_master.gold.etl_log;

-- Drop Gold Layer Views
DROP VIEW IF EXISTS ticket_master.gold.mv_events_by_date_venue;
DROP VIEW IF EXISTS ticket_master.gold.mv_events_by_attraction;
DROP VIEW IF EXISTS ticket_master.gold.mv_monthly_summary;

-- ============================================================
-- Drop Silver Layer Tables (Normalized)
-- ============================================================
-- Drop bridge tables first (have FKs)
DROP TABLE IF EXISTS ticket_master.silver.event_venues;
DROP TABLE IF EXISTS ticket_master.silver.event_attractions;

-- Drop fact table
DROP TABLE IF EXISTS ticket_master.silver.events;

-- Drop dimension tables
DROP TABLE IF EXISTS ticket_master.silver.venues;
DROP TABLE IF EXISTS ticket_master.silver.attractions;
DROP TABLE IF EXISTS ticket_master.silver.classifications;
DROP TABLE IF EXISTS ticket_master.silver.markets;

-- ============================================================
-- Drop Bronze Layer Tables (Raw)
-- ============================================================
DROP TABLE IF EXISTS ticket_master.bronze.events_raw;
DROP TABLE IF EXISTS ticket_master.bronze.venues_raw;
DROP TABLE IF EXISTS ticket_master.bronze.attractions_raw;
DROP TABLE IF EXISTS ticket_master.bronze.classifications_raw;

-- ============================================================
-- Drop Volumes (Deletes all raw JSON files and checkpoints!)
-- ============================================================
-- WARNING: This will delete all raw data files, checkpoints, and metadata!
DROP VOLUME IF EXISTS ticket_master.bronze.raw_data CASCADE;

-- ============================================================
-- Verification - Show remaining resources
-- ============================================================
SHOW TABLES IN ticket_master.bronze;
SHOW TABLES IN ticket_master.silver;
SHOW TABLES IN ticket_master.gold;
SHOW VOLUMES IN ticket_master.bronze;

-- ============================================================
-- Summary
-- ============================================================
-- This script removed:
-- - Bronze: 4 tables + 1 volume (with all files)
-- - Silver: 7 tables
-- - Gold: 8 tables + 3 views
-- Total: 19 tables, 1 volume, 3 views
-- ============================================================

