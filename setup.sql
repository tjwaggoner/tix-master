-- ============================================================
-- Ticketmaster Medallion Architecture - Setup SQL
-- Run this in Databricks SQL Warehouse UI
-- ============================================================

-- Step 1: Create Catalog
CREATE CATALOG IF NOT EXISTS ticketmaster_dev
COMMENT 'Ticketmaster Medallion Architecture - Development';

-- Step 2: Create Schemas
CREATE SCHEMA IF NOT EXISTS ticketmaster_dev.bronze
COMMENT 'Bronze layer - raw data from Ticketmaster API with Auto Loader';

CREATE SCHEMA IF NOT EXISTS ticketmaster_dev.silver
COMMENT 'Silver layer - normalized relational tables with PK/FK constraints and liquid clustering';

CREATE SCHEMA IF NOT EXISTS ticketmaster_dev.gold
COMMENT 'Gold layer - star schema for BI with identity keys and liquid clustering';

-- Step 3: Create Volume
CREATE VOLUME IF NOT EXISTS ticketmaster_dev.bronze.raw_data
COMMENT 'Volume for staging raw JSON from Ticketmaster API';

-- Step 4: Create ETL Log Table
CREATE TABLE IF NOT EXISTS ticketmaster_dev.gold.etl_log (
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
) COMMENT 'Tracks execution of ETL stored procedures including performance metrics and errors';

-- Step 5: Verify Setup
SHOW CATALOGS LIKE 'ticketmaster_dev';
SHOW SCHEMAS IN ticketmaster_dev;
SHOW VOLUMES IN ticketmaster_dev.bronze;
SHOW TABLES IN ticketmaster_dev.gold;

-- ============================================================
-- Setup Complete!
-- ============================================================
-- Next: Run the notebooks in order:
-- 1. bronze/bronze_auto_loader
-- 2. silver/silver_transformations
-- 3. gold/gold_star_schema
-- ============================================================
