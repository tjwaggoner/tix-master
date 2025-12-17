# Databricks notebook source
"""
Gold Layer - Star Schema with Identity Keys

This notebook creates consumption-ready star schemas with:
- Identity surrogate keys
- Slowly Changing Dimensions (SCD Type 2)
- Fact tables with FK references
- Materialized views for common aggregates
- Optimized for BI tools and SQL Warehouse
"""

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Layer: Star Schema Design
# MAGIC
# MAGIC Implements dimensional modeling with:
# MAGIC - **Fact Tables**: fact_events (grain: one row per event)
# MAGIC - **Dimensions**: dim_venue, dim_attraction, dim_date, dim_classification, dim_market
# MAGIC - **Identity Keys**: Auto-incrementing surrogate keys
# MAGIC - **Materialized Views**: Pre-aggregated metrics for BI
# MAGIC
# MAGIC This design enables:
# MAGIC - Fast BI queries with star schema joins
# MAGIC - Query optimizer benefits from identity keys
# MAGIC - Easy integration with reporting tools

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, coalesce, when, date_format, year, month, dayofmonth,
    dayofweek, quarter, weekofyear, last_day, concat_ws, count, sum as spark_sum,
    avg, min as spark_min, max as spark_max, countDistinct
)
from pyspark.sql.types import *
from delta.tables import DeltaTable
from datetime import datetime, timedelta

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = "ticket_master"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

# Set catalog context to avoid Hive Metastore errors
spark.sql(f"USE CATALOG {CATALOG}")
print(f"✓ Using catalog: {spark.catalog.currentCatalog()}")

# Create Gold schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}")
print(f"✓ Gold schema ready: {CATALOG}.{GOLD_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def add_primary_key_if_not_exists(table_name: str, constraint_name: str, pk_columns: list):
    """
    Add a primary key constraint if it doesn't already exist.
    Also ensures the columns are set to NOT NULL first.
    
    Args:
        table_name: Full table name (catalog.schema.table)
        constraint_name: Name of the constraint
        pk_columns: List of column names that make up the primary key
    """
    # Check if constraint already exists using information_schema
    catalog, schema, table = table_name.split(".")
    constraints = spark.sql(f"""
        SELECT constraint_name
        FROM {catalog}.information_schema.table_constraints
        WHERE table_schema = '{schema}'
          AND table_name = '{table}'
          AND constraint_type = 'PRIMARY KEY'
          AND constraint_name = '{constraint_name}'
    """).collect()
    
    if constraints:
        print(f"  ⚠️  Constraint '{constraint_name}' already exists on {table_name}, skipping")
        return
    
    # Set columns as NOT NULL
    for col_name in pk_columns:
        try:
            spark.sql(
                f"ALTER TABLE {table_name} ALTER COLUMN {col_name} SET NOT NULL"
            )
        except Exception as e:
            if "DELTA_COLUMN_ALREADY_NOT_NULLABLE" in str(e):
                pass
            else:
                raise
    
    # Add primary key constraint with RELY
    pk_cols_str = ", ".join(pk_columns)
    spark.sql(
        f"ALTER TABLE {table_name} ADD CONSTRAINT {constraint_name} PRIMARY KEY ({pk_cols_str}) RELY"
    )
    print(f"  ✓ Added constraint '{constraint_name}' on {table_name}")

def add_foreign_key_if_not_exists(table_name: str, constraint_name: str, fk_columns: list, reference_table: str, reference_columns: list):
    """
    Add a foreign key constraint if it doesn't already exist.
    
    Args:
        table_name: Full table name (catalog.schema.table)
        constraint_name: Name of the constraint
        fk_columns: List of column names that make up the foreign key
        reference_table: Full reference table name (catalog.schema.table)
        reference_columns: List of column names in the reference table
    """
    # Check if constraint already exists using information_schema
    catalog, schema, table = table_name.split(".")
    constraints = spark.sql(f"""
        SELECT constraint_name
        FROM {catalog}.information_schema.table_constraints
        WHERE table_schema = '{schema}'
          AND table_name = '{table}'
          AND constraint_type = 'FOREIGN KEY'
          AND constraint_name = '{constraint_name}'
    """).collect()
    
    if constraints:
        print(f"  ⚠️  Constraint '{constraint_name}' already exists on {table_name}, skipping")
        return
    
    # Add foreign key constraint
    fk_cols_str = ", ".join(fk_columns)
    ref_cols_str = ", ".join(reference_columns)
    spark.sql(
        f"ALTER TABLE {table_name} ADD CONSTRAINT {constraint_name} FOREIGN KEY ({fk_cols_str}) REFERENCES {reference_table} ({ref_cols_str})"
    )
    print(f"  ✓ Added constraint '{constraint_name}' on {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension Table: Date Dimension

# COMMAND ----------

def create_dim_date():
    """
    Create a date dimension table with comprehensive date attributes
    Uses IDENTITY column for surrogate key
    """

    # Generate date range (e.g., 2020-2030)
    start_date = datetime(2020, 1, 1)
    end_date = datetime(2030, 12, 31)
    date_range = [(start_date + timedelta(days=x),) for x in range((end_date - start_date).days + 1)]

    # Create DataFrame
    dates_df = spark.createDataFrame(date_range, ["date_value"])

    dim_date = (
        dates_df
        .withColumn("date_key", date_format(col("date_value"), "yyyyMMdd").cast("int"))
        .withColumn("year", year(col("date_value")))
        .withColumn("month", month(col("date_value")))
        .withColumn("day", dayofmonth(col("date_value")))
        .withColumn("quarter", quarter(col("date_value")))
        .withColumn("week_of_year", weekofyear(col("date_value")))
        .withColumn("day_of_week", dayofweek(col("date_value")))
        .withColumn("month_name", date_format(col("date_value"), "MMMM"))
        .withColumn("day_name", date_format(col("date_value"), "EEEE"))
        .withColumn("is_weekend", when(col("day_of_week").isin([1, 7]), lit(True)).otherwise(lit(False)))
        .withColumn("month_end_date", last_day(col("date_value")))
    )

    table_name = f"{CATALOG}.{GOLD_SCHEMA}.dim_date"

    # Write with identity column
    dim_date.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(table_name)

    # Add primary key on date_key
    # Add primary key constraint (if not exists)
    add_primary_key_if_not_exists(table_name, "dim_date_pk", ["date_key"])

    print(f"✓ Created {table_name} with {dim_date.count():,} records")

# COMMAND ----------

create_dim_date()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension Table: Venue Dimension

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create dim_venue with IDENTITY surrogate key
# MAGIC CREATE OR REPLACE TABLE ticket_master.gold.dim_venue (
# MAGIC   venue_sk BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   venue_id STRING NOT NULL,
# MAGIC   venue_name STRING,
# MAGIC   venue_type STRING,
# MAGIC   city STRING,
# MAGIC   state STRING,
# MAGIC   state_code STRING,
# MAGIC   country STRING,
# MAGIC   country_code STRING,
# MAGIC   postal_code STRING,
# MAGIC   address_line1 STRING,
# MAGIC   latitude DOUBLE,
# MAGIC   longitude DOUBLE,
# MAGIC   timezone STRING,
# MAGIC   venue_url STRING,
# MAGIC   valid_from TIMESTAMP,
# MAGIC   valid_to TIMESTAMP,
# MAGIC   is_current BOOLEAN,
# MAGIC   CONSTRAINT dim_venue_pk PRIMARY KEY (venue_sk)
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Populate dim_venue from Silver
# MAGIC INSERT INTO ticket_master.gold.dim_venue (
# MAGIC   venue_id, venue_name, venue_type, city, state, state_code,
# MAGIC   country, country_code, postal_code, address_line1,
# MAGIC   latitude, longitude, timezone, venue_url,
# MAGIC   valid_from, valid_to, is_current
# MAGIC )
# MAGIC SELECT
# MAGIC   venue_id,
# MAGIC   venue_name,
# MAGIC   venue_type,
# MAGIC   city,
# MAGIC   state,
# MAGIC   state_code,
# MAGIC   country,
# MAGIC   country_code,
# MAGIC   postal_code,
# MAGIC   address_line1,
# MAGIC   latitude,
# MAGIC   longitude,
# MAGIC   timezone,
# MAGIC   venue_url,
# MAGIC   _ingestion_timestamp as valid_from,
# MAGIC   CAST(NULL AS TIMESTAMP) as valid_to,
# MAGIC   TRUE as is_current
# MAGIC FROM ticket_master.silver.venues;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension Table: Attraction Dimension

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create dim_attraction with IDENTITY surrogate key
# MAGIC CREATE OR REPLACE TABLE ticket_master.gold.dim_attraction (
# MAGIC   attraction_sk BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   attraction_id STRING NOT NULL,
# MAGIC   attraction_name STRING,
# MAGIC   attraction_type STRING,
# MAGIC   segment_id STRING,
# MAGIC   segment_name STRING,
# MAGIC   genre_id STRING,
# MAGIC   genre_name STRING,
# MAGIC   attraction_url STRING,
# MAGIC   is_test BOOLEAN,
# MAGIC   valid_from TIMESTAMP,
# MAGIC   valid_to TIMESTAMP,
# MAGIC   is_current BOOLEAN,
# MAGIC   CONSTRAINT dim_attraction_pk PRIMARY KEY (attraction_sk)
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Populate dim_attraction
# MAGIC INSERT INTO ticket_master.gold.dim_attraction (
# MAGIC   attraction_id, attraction_name, attraction_type,
# MAGIC   segment_id, segment_name, genre_id, genre_name,
# MAGIC   attraction_url, is_test,
# MAGIC   valid_from, valid_to, is_current
# MAGIC )
# MAGIC SELECT
# MAGIC   attraction_id,
# MAGIC   attraction_name,
# MAGIC   attraction_type,
# MAGIC   segment_id,
# MAGIC   segment_name,
# MAGIC   genre_id,
# MAGIC   genre_name,
# MAGIC   attraction_url,
# MAGIC   is_test,
# MAGIC   _ingestion_timestamp as valid_from,
# MAGIC   CAST(NULL AS TIMESTAMP) as valid_to,
# MAGIC   TRUE as is_current
# MAGIC FROM ticket_master.silver.attractions;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension Table: Classification Dimension

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create dim_classification
# MAGIC CREATE OR REPLACE TABLE ticket_master.gold.dim_classification (
# MAGIC   classification_sk BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
# MAGIC   classification_id STRING NOT NULL,
# MAGIC   segment_id STRING,
# MAGIC   segment_name STRING,
# MAGIC   genre_id STRING,
# MAGIC   genre_name STRING,
# MAGIC   subgenre_id STRING,
# MAGIC   subgenre_name STRING,
# MAGIC   type_id STRING,
# MAGIC   type_name STRING,
# MAGIC   subtype_id STRING,
# MAGIC   subtype_name STRING,
# MAGIC   CONSTRAINT dim_classification_pk PRIMARY KEY (classification_sk)
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Populate dim_classification (excluding IDENTITY column)
# MAGIC -- Note: Only segment, type, and family fields exist in Ticketmaster API
# MAGIC INSERT INTO ticket_master.gold.dim_classification (
# MAGIC   classification_id,
# MAGIC   segment_id,
# MAGIC   segment_name,
# MAGIC   type_id,
# MAGIC   type_name
# MAGIC )
# MAGIC SELECT
# MAGIC   classification_id,
# MAGIC   segment_id,
# MAGIC   segment_name,
# MAGIC   type_id,
# MAGIC   type_name
# MAGIC FROM ticket_master.silver.classifications;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension Table: Market Dimension

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create dim_market
# MAGIC CREATE OR REPLACE TABLE ticket_master.gold.dim_market (
# MAGIC   market_sk BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   market_id STRING NOT NULL,
# MAGIC   market_name STRING,
# MAGIC   CONSTRAINT dim_market_pk PRIMARY KEY (market_sk)
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Populate dim_market (excluding IDENTITY column)
# MAGIC INSERT INTO ticket_master.gold.dim_market (
# MAGIC   market_id,
# MAGIC   market_name
# MAGIC )
# MAGIC SELECT
# MAGIC   market_id,
# MAGIC   market_name
# MAGIC FROM ticket_master.silver.markets;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact Table: Events Fact

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop existing table to ensure clean recreation with updated schema
# MAGIC DROP TABLE IF EXISTS ticket_master.gold.fact_events;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create fact_events with foreign keys to dimensions and liquid clustering
# MAGIC -- Using liquid clustering (DBR 15.2+) instead of partitioning for better performance
# MAGIC CREATE TABLE ticket_master.gold.fact_events (
# MAGIC   event_sk BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   event_id STRING NOT NULL,
# MAGIC   event_name STRING,
# MAGIC   event_type STRING,
# MAGIC   event_date_key INT,
# MAGIC   venue_sk BIGINT,
# MAGIC   attraction_sk BIGINT,
# MAGIC   classification_sk BIGINT,
# MAGIC   event_datetime TIMESTAMP,
# MAGIC   event_time STRING,
# MAGIC   event_timezone STRING,
# MAGIC   price_min DOUBLE,
# MAGIC   price_max DOUBLE,
# MAGIC   price_currency STRING,
# MAGIC   status_code STRING,
# MAGIC   sales_start_datetime TIMESTAMP,
# MAGIC   sales_end_datetime TIMESTAMP,
# MAGIC   is_test BOOLEAN,
# MAGIC   event_url STRING,
# MAGIC   CONSTRAINT fact_events_pk PRIMARY KEY (event_sk),
# MAGIC   CONSTRAINT fact_events_date_fk FOREIGN KEY (event_date_key)
# MAGIC     REFERENCES ticket_master.gold.dim_date(date_key),
# MAGIC   CONSTRAINT fact_events_venue_fk FOREIGN KEY (venue_sk)
# MAGIC     REFERENCES ticket_master.gold.dim_venue(venue_sk),
# MAGIC   CONSTRAINT fact_events_attraction_fk FOREIGN KEY (attraction_sk)
# MAGIC     REFERENCES ticket_master.gold.dim_attraction(attraction_sk)
# MAGIC )
# MAGIC CLUSTER BY (event_date_key, venue_sk);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- MERGE fact_events for upsert capability (insert new, update existing)
# MAGIC MERGE INTO ticket_master.gold.fact_events AS t
# MAGIC USING (
# MAGIC   SELECT
# MAGIC     e.event_id,
# MAGIC     e.event_name,
# MAGIC     e.event_type,
# MAGIC     CAST(date_format(e.event_date, 'yyyyMMdd') AS INT) AS event_date_key,
# MAGIC     dv.venue_sk,
# MAGIC     da.attraction_sk,
# MAGIC     CAST(NULL AS BIGINT) AS classification_sk, -- placeholder for future join
# MAGIC     e.event_datetime,
# MAGIC     e.event_time,
# MAGIC     e.event_timezone,
# MAGIC     e.price_min,
# MAGIC     e.price_max,
# MAGIC     e.price_currency,
# MAGIC     e.status_code,
# MAGIC     e.sales_start_datetime,
# MAGIC     e.sales_end_datetime,
# MAGIC     e.is_test,
# MAGIC     e.event_url
# MAGIC   FROM ticket_master.silver.events e
# MAGIC   LEFT JOIN ticket_master.silver.event_venues ev
# MAGIC     ON e.event_id = ev.event_id
# MAGIC   LEFT JOIN ticket_master.gold.dim_venue dv
# MAGIC     ON ev.venue_id = dv.venue_id AND dv.is_current = TRUE
# MAGIC   LEFT JOIN ticket_master.silver.event_attractions ea
# MAGIC     ON e.event_id = ea.event_id
# MAGIC   LEFT JOIN ticket_master.gold.dim_attraction da
# MAGIC     ON ea.attraction_id = da.attraction_id AND da.is_current = TRUE
# MAGIC ) AS s
# MAGIC -- Match on event_id, venue_sk, and attraction_sk (preserves grain)
# MAGIC ON  t.event_id       = s.event_id
# MAGIC AND t.venue_sk      <=> s.venue_sk       -- null-safe equality
# MAGIC AND t.attraction_sk <=> s.attraction_sk  -- null-safe equality
# MAGIC 
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC   t.event_name            = s.event_name,
# MAGIC   t.event_type            = s.event_type,
# MAGIC   t.event_date_key        = s.event_date_key,
# MAGIC   t.venue_sk              = s.venue_sk,
# MAGIC   t.attraction_sk         = s.attraction_sk,
# MAGIC   t.classification_sk     = s.classification_sk,
# MAGIC   t.event_datetime        = s.event_datetime,
# MAGIC   t.event_time            = s.event_time,
# MAGIC   t.event_timezone        = s.event_timezone,
# MAGIC   t.price_min             = s.price_min,
# MAGIC   t.price_max             = s.price_max,
# MAGIC   t.price_currency        = s.price_currency,
# MAGIC   t.status_code           = s.status_code,
# MAGIC   t.sales_start_datetime  = s.sales_start_datetime,
# MAGIC   t.sales_end_datetime    = s.sales_end_datetime,
# MAGIC   t.is_test               = s.is_test,
# MAGIC   t.event_url             = s.event_url
# MAGIC 
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC   event_id,
# MAGIC   event_name,
# MAGIC   event_type,
# MAGIC   event_date_key,
# MAGIC   venue_sk,
# MAGIC   attraction_sk,
# MAGIC   classification_sk,
# MAGIC   event_datetime,
# MAGIC   event_time,
# MAGIC   event_timezone,
# MAGIC   price_min,
# MAGIC   price_max,
# MAGIC   price_currency,
# MAGIC   status_code,
# MAGIC   sales_start_datetime,
# MAGIC   sales_end_datetime,
# MAGIC   is_test,
# MAGIC   event_url
# MAGIC ) VALUES (
# MAGIC   s.event_id,
# MAGIC   s.event_name,
# MAGIC   s.event_type,
# MAGIC   s.event_date_key,
# MAGIC   s.venue_sk,
# MAGIC   s.attraction_sk,
# MAGIC   s.classification_sk,
# MAGIC   s.event_datetime,
# MAGIC   s.event_time,
# MAGIC   s.event_timezone,
# MAGIC   s.price_min,
# MAGIC   s.price_max,
# MAGIC   s.price_currency,
# MAGIC   s.status_code,
# MAGIC   s.sales_start_datetime,
# MAGIC   s.sales_end_datetime,
# MAGIC   s.is_test,
# MAGIC   s.event_url
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## Materialized Views for Common Aggregates

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create materialized view: Events by Date and Venue
# MAGIC CREATE OR REPLACE VIEW ticket_master.gold.mv_events_by_date_venue AS
# MAGIC SELECT
# MAGIC   d.date_value,
# MAGIC   d.year,
# MAGIC   d.month,
# MAGIC   d.month_name,
# MAGIC   d.day_name,
# MAGIC   v.city,
# MAGIC   v.state,
# MAGIC   v.country,
# MAGIC   COUNT(DISTINCT f.event_id) as event_count,
# MAGIC   COUNT(DISTINCT v.venue_sk) as venue_count,
# MAGIC   AVG(f.price_min) as avg_price_min,
# MAGIC   AVG(f.price_max) as avg_price_max,
# MAGIC   MIN(f.sales_start_datetime) as earliest_sale_start
# MAGIC FROM ticket_master.gold.fact_events f
# MAGIC INNER JOIN ticket_master.gold.dim_date d ON f.event_date_key = d.date_key
# MAGIC INNER JOIN ticket_master.gold.dim_venue v ON f.venue_sk = v.venue_sk
# MAGIC GROUP BY
# MAGIC   d.date_value, d.year, d.month, d.month_name, d.day_name,
# MAGIC   v.city, v.state, v.country;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create materialized view: Events by Attraction Type
# MAGIC CREATE OR REPLACE VIEW ticket_master.gold.mv_events_by_attraction AS
# MAGIC SELECT
# MAGIC   a.attraction_name,
# MAGIC   a.attraction_type,
# MAGIC   a.segment_name,
# MAGIC   a.genre_name,
# MAGIC   COUNT(DISTINCT f.event_id) as total_events,
# MAGIC   COUNT(DISTINCT v.city) as cities_count,
# MAGIC   COUNT(DISTINCT v.state) as states_count,
# MAGIC   MIN(d.date_value) as first_event_date,
# MAGIC   MAX(d.date_value) as last_event_date,
# MAGIC   AVG(f.price_max) as avg_max_price
# MAGIC FROM ticket_master.gold.fact_events f
# MAGIC INNER JOIN ticket_master.gold.dim_attraction a ON f.attraction_sk = a.attraction_sk
# MAGIC INNER JOIN ticket_master.gold.dim_venue v ON f.venue_sk = v.venue_sk
# MAGIC INNER JOIN ticket_master.gold.dim_date d ON f.event_date_key = d.date_key
# MAGIC GROUP BY
# MAGIC   a.attraction_name, a.attraction_type, a.segment_name, a.genre_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create materialized view: Monthly Event Summary
# MAGIC CREATE OR REPLACE VIEW ticket_master.gold.mv_monthly_summary AS
# MAGIC SELECT
# MAGIC   d.year,
# MAGIC   d.month,
# MAGIC   d.month_name,
# MAGIC   d.quarter,
# MAGIC   COUNT(DISTINCT f.event_id) as total_events,
# MAGIC   COUNT(DISTINCT f.venue_sk) as unique_venues,
# MAGIC   COUNT(DISTINCT f.attraction_sk) as unique_attractions,
# MAGIC   AVG(f.price_min) as avg_min_price,
# MAGIC   AVG(f.price_max) as avg_max_price,
# MAGIC   COUNT(DISTINCT CASE WHEN d.is_weekend THEN f.event_id END) as weekend_events,
# MAGIC   COUNT(DISTINCT CASE WHEN NOT d.is_weekend THEN f.event_id END) as weekday_events
# MAGIC FROM ticket_master.gold.fact_events f
# MAGIC INNER JOIN ticket_master.gold.dim_date d ON f.event_date_key = d.date_key
# MAGIC GROUP BY
# MAGIC   d.year, d.month, d.month_name, d.quarter;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Gold Layer

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show all gold tables
# MAGIC SHOW TABLES IN ticket_master.gold;

# COMMAND ----------

# Display record counts
gold_tables = [
    "fact_events", "dim_venue", "dim_attraction",
    "dim_date", "dim_classification", "dim_market"
]

for table in gold_tables:
    try:
        count = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.{table}").count()
        print(f"ticket_master.gold.{table}: {count:,} records")
    except:
        print(f"ticket_master.gold.{table}: Not found")

# COMMAND ----------
