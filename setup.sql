-- ============================================================
-- Ticketmaster Medallion Architecture - Setup SQL
-- Run this in Databricks SQL Warehouse UI
-- ============================================================

-- IMPORTANT: First check if the catalog already exists
-- If this is a new workspace without catalogs, you may need admin to enable Unity Catalog

-- Step 1: Create Catalog (requires CREATE CATALOG permission)
CREATE CATALOG IF NOT EXISTS ticketmaster_dev
COMMENT 'Ticketmaster Medallion Architecture - Development';

-- Step 2: Use the new catalog (this avoids hive_metastore errors)
USE CATALOG ticketmaster_dev;

-- Step 3: Create Schemas (now we're in the catalog context)
CREATE SCHEMA IF NOT EXISTS bronze
COMMENT 'Bronze layer - raw data from Ticketmaster API with Auto Loader';

CREATE SCHEMA IF NOT EXISTS silver
COMMENT 'Silver layer - normalized relational tables with PK/FK constraints and liquid clustering';

CREATE SCHEMA IF NOT EXISTS gold
COMMENT 'Gold layer - star schema for BI with identity keys and liquid clustering';

-- Step 4: Create Volume
CREATE VOLUME IF NOT EXISTS bronze.raw_data
COMMENT 'Volume for staging raw JSON from Ticketmaster API';

-- Step 5: Create ETL Log Table
CREATE TABLE IF NOT EXISTS gold.etl_log (
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

-- Step 6: Verify Setup
SHOW CATALOGS LIKE 'ticketmaster_dev';
SHOW SCHEMAS;
SHOW VOLUMES IN bronze;
SHOW TABLES IN gold;

-- ============================================================
-- Setup Complete!
-- ============================================================
-- Next: Run the notebooks in order:
-- 1. bronze/bronze_auto_loader
-- 2. silver/silver_transformations
-- 3. gold/gold_star_schema
-- ============================================================
