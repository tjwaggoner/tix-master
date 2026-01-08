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
    avg, min as spark_min, max as spark_max, countDistinct, row_number
)
from pyspark.sql.types import *
from pyspark.sql.window import Window
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
    Uses surrogate key (date_sk) and natural date key (date_value)
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

    # Add surrogate key using row_number over date_value ordering
    window_spec = Window.orderBy("date_value")
    dim_date = dim_date.withColumn("date_sk", row_number().over(window_spec))

    # Reorder columns to put date_sk first, then date_value
    dim_date = dim_date.select(
        "date_sk", "date_value", "date_key", "year", "month", "day",
        "quarter", "week_of_year", "day_of_week", "month_name",
        "day_name", "is_weekend", "month_end_date"
    )

    table_name = f"{CATALOG}.{GOLD_SCHEMA}.dim_date"

    # Write with surrogate key
    dim_date.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(table_name)

    # Add primary key on date_sk (surrogate key)
    # Add primary key constraint (if not exists)
    add_primary_key_if_not_exists(table_name, "dim_date_pk", ["date_sk"])

    print(f"✓ Created {table_name} with {dim_date.count():,} records")

# COMMAND ----------

create_dim_date()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension Table: Venue Dimension

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create dim_venue with SCD Type 2 for tracking historical changes
# MAGIC CREATE TABLE IF NOT EXISTS ticket_master.gold.dim_venue (
# MAGIC   venue_sk STRING NOT NULL,
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
# MAGIC   markets VARIANT,
# MAGIC   valid_from TIMESTAMP NOT NULL,
# MAGIC   valid_to TIMESTAMP,
# MAGIC   is_current BOOLEAN NOT NULL,
# MAGIC   CONSTRAINT dim_venue_pk PRIMARY KEY (venue_sk, valid_from)
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SCD Type 2: Expire old records where attributes have changed
# MAGIC MERGE INTO ticket_master.gold.dim_venue AS t
# MAGIC USING ticket_master.silver.venues AS s
# MAGIC ON t.venue_id = s.venue_id AND t.is_current = TRUE
# MAGIC WHEN MATCHED AND (
# MAGIC   t.venue_name <> s.venue_name OR
# MAGIC   t.venue_type <> s.venue_type OR
# MAGIC   t.city <> s.city OR
# MAGIC   t.state <> s.state OR
# MAGIC   t.state_code <> s.state_code OR
# MAGIC   t.country <> s.country OR
# MAGIC   t.country_code <> s.country_code OR
# MAGIC   t.postal_code <> s.postal_code OR
# MAGIC   t.address_line1 <> s.address_line1 OR
# MAGIC   t.latitude <> s.latitude OR
# MAGIC   t.longitude <> s.longitude OR
# MAGIC   t.timezone <> s.timezone OR
# MAGIC   t.venue_url <> s.venue_url
# MAGIC ) THEN UPDATE SET
# MAGIC   t.is_current = FALSE,
# MAGIC   t.valid_to = current_timestamp();
# MAGIC
# MAGIC -- SCD Type 2: Insert new versions for changed records and new records
# MAGIC INSERT INTO ticket_master.gold.dim_venue (
# MAGIC   venue_sk, venue_id, venue_name, venue_type, city, state, state_code,
# MAGIC   country, country_code, postal_code, address_line1,
# MAGIC   latitude, longitude, timezone, venue_url, markets,
# MAGIC   valid_from, valid_to, is_current
# MAGIC )
# MAGIC SELECT
# MAGIC   s.venue_sk,
# MAGIC   s.venue_id,
# MAGIC   s.venue_name,
# MAGIC   s.venue_type,
# MAGIC   s.city,
# MAGIC   s.state,
# MAGIC   s.state_code,
# MAGIC   s.country,
# MAGIC   s.country_code,
# MAGIC   s.postal_code,
# MAGIC   s.address_line1,
# MAGIC   s.latitude,
# MAGIC   s.longitude,
# MAGIC   s.timezone,
# MAGIC   s.venue_url,
# MAGIC   s.markets,
# MAGIC   current_timestamp() as valid_from,
# MAGIC   CAST(NULL AS TIMESTAMP) as valid_to,
# MAGIC   TRUE as is_current
# MAGIC FROM ticket_master.silver.venues s
# MAGIC LEFT JOIN ticket_master.gold.dim_venue t
# MAGIC   ON s.venue_id = t.venue_id AND t.is_current = TRUE
# MAGIC WHERE t.venue_id IS NULL  -- New records
# MAGIC    OR (  -- Changed records
# MAGIC      t.venue_name <> s.venue_name OR
# MAGIC      t.venue_type <> s.venue_type OR
# MAGIC      t.city <> s.city OR
# MAGIC      t.state <> s.state OR
# MAGIC      t.state_code <> s.state_code OR
# MAGIC      t.country <> s.country OR
# MAGIC      t.country_code <> s.country_code OR
# MAGIC      t.postal_code <> s.postal_code OR
# MAGIC      t.address_line1 <> s.address_line1 OR
# MAGIC      t.latitude <> s.latitude OR
# MAGIC      t.longitude <> s.longitude OR
# MAGIC      t.timezone <> s.timezone OR
# MAGIC      t.venue_url <> s.venue_url
# MAGIC    );

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension Table: Attraction Dimension

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create dim_attraction with SCD Type 2 for tracking historical changes
# MAGIC CREATE TABLE IF NOT EXISTS ticket_master.gold.dim_attraction (
# MAGIC   attraction_sk STRING NOT NULL,
# MAGIC   attraction_id STRING NOT NULL,
# MAGIC   attraction_name STRING,
# MAGIC   attraction_type STRING,
# MAGIC   segment_id STRING,
# MAGIC   segment_name STRING,
# MAGIC   genre_id STRING,
# MAGIC   genre_name STRING,
# MAGIC   attraction_url STRING,
# MAGIC   is_test BOOLEAN,
# MAGIC   valid_from TIMESTAMP NOT NULL,
# MAGIC   valid_to TIMESTAMP,
# MAGIC   is_current BOOLEAN NOT NULL,
# MAGIC   CONSTRAINT dim_attraction_pk PRIMARY KEY (attraction_sk, valid_from)
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SCD Type 2: Expire old records where attributes have changed
# MAGIC MERGE INTO ticket_master.gold.dim_attraction AS t
# MAGIC USING ticket_master.silver.attractions AS s
# MAGIC ON t.attraction_id = s.attraction_id AND t.is_current = TRUE
# MAGIC WHEN MATCHED AND (
# MAGIC   t.attraction_name <> s.attraction_name OR
# MAGIC   t.attraction_type <> s.attraction_type OR
# MAGIC   t.segment_id <> s.segment_id OR
# MAGIC   t.segment_name <> s.segment_name OR
# MAGIC   t.genre_id <> s.genre_id OR
# MAGIC   t.genre_name <> s.genre_name OR
# MAGIC   t.attraction_url <> s.attraction_url OR
# MAGIC   t.is_test <> s.is_test
# MAGIC ) THEN UPDATE SET
# MAGIC   t.is_current = FALSE,
# MAGIC   t.valid_to = current_timestamp();
# MAGIC
# MAGIC -- SCD Type 2: Insert new versions for changed records and new records
# MAGIC INSERT INTO ticket_master.gold.dim_attraction (
# MAGIC   attraction_sk, attraction_id, attraction_name, attraction_type,
# MAGIC   segment_id, segment_name, genre_id, genre_name,
# MAGIC   attraction_url, is_test,
# MAGIC   valid_from, valid_to, is_current
# MAGIC )
# MAGIC SELECT
# MAGIC   s.attraction_sk,
# MAGIC   s.attraction_id,
# MAGIC   s.attraction_name,
# MAGIC   s.attraction_type,
# MAGIC   s.segment_id,
# MAGIC   s.segment_name,
# MAGIC   s.genre_id,
# MAGIC   s.genre_name,
# MAGIC   s.attraction_url,
# MAGIC   s.is_test,
# MAGIC   current_timestamp() as valid_from,
# MAGIC   CAST(NULL AS TIMESTAMP) as valid_to,
# MAGIC   TRUE as is_current
# MAGIC FROM ticket_master.silver.attractions s
# MAGIC LEFT JOIN ticket_master.gold.dim_attraction t
# MAGIC   ON s.attraction_id = t.attraction_id AND t.is_current = TRUE
# MAGIC WHERE t.attraction_id IS NULL  -- New records
# MAGIC    OR (  -- Changed records
# MAGIC      t.attraction_name <> s.attraction_name OR
# MAGIC      t.attraction_type <> s.attraction_type OR
# MAGIC      t.segment_id <> s.segment_id OR
# MAGIC      t.segment_name <> s.segment_name OR
# MAGIC      t.genre_id <> s.genre_id OR
# MAGIC      t.genre_name <> s.genre_name OR
# MAGIC      t.attraction_url <> s.attraction_url OR
# MAGIC      t.is_test <> s.is_test
# MAGIC    );

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension Table: Classification Dimension

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create dim_classification with SCD Type 2 for tracking historical changes
# MAGIC CREATE TABLE IF NOT EXISTS ticket_master.gold.dim_classification (
# MAGIC   classification_sk STRING NOT NULL,
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
# MAGIC   valid_from TIMESTAMP NOT NULL,
# MAGIC   valid_to TIMESTAMP,
# MAGIC   is_current BOOLEAN NOT NULL,
# MAGIC   CONSTRAINT dim_classification_pk PRIMARY KEY (classification_sk, valid_from)
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SCD Type 2: Expire old records where attributes have changed
# MAGIC -- Note: Only segment and type fields exist in Ticketmaster API
# MAGIC MERGE INTO ticket_master.gold.dim_classification AS t
# MAGIC USING (
# MAGIC   SELECT
# MAGIC     classification_id as classification_sk,
# MAGIC     classification_id,
# MAGIC     segment_id,
# MAGIC     segment_name,
# MAGIC     type_id,
# MAGIC     type_name
# MAGIC   FROM ticket_master.silver.classifications
# MAGIC ) AS s
# MAGIC ON t.classification_id = s.classification_id AND t.is_current = TRUE
# MAGIC WHEN MATCHED AND (
# MAGIC   t.segment_id <> s.segment_id OR
# MAGIC   t.segment_name <> s.segment_name OR
# MAGIC   t.type_id <> s.type_id OR
# MAGIC   t.type_name <> s.type_name
# MAGIC ) THEN UPDATE SET
# MAGIC   t.is_current = FALSE,
# MAGIC   t.valid_to = current_timestamp();
# MAGIC
# MAGIC -- SCD Type 2: Insert new versions for changed records and new records
# MAGIC INSERT INTO ticket_master.gold.dim_classification (
# MAGIC   classification_sk,
# MAGIC   classification_id,
# MAGIC   segment_id,
# MAGIC   segment_name,
# MAGIC   type_id,
# MAGIC   type_name,
# MAGIC   valid_from, valid_to, is_current
# MAGIC )
# MAGIC SELECT
# MAGIC   s.classification_sk,
# MAGIC   s.classification_id,
# MAGIC   s.segment_id,
# MAGIC   s.segment_name,
# MAGIC   s.type_id,
# MAGIC   s.type_name,
# MAGIC   current_timestamp() as valid_from,
# MAGIC   CAST(NULL AS TIMESTAMP) as valid_to,
# MAGIC   TRUE as is_current
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     classification_id as classification_sk,
# MAGIC     classification_id,
# MAGIC     segment_id,
# MAGIC     segment_name,
# MAGIC     type_id,
# MAGIC     type_name
# MAGIC   FROM ticket_master.silver.classifications
# MAGIC ) s
# MAGIC LEFT JOIN ticket_master.gold.dim_classification t
# MAGIC   ON s.classification_id = t.classification_id AND t.is_current = TRUE
# MAGIC WHERE t.classification_id IS NULL  -- New records
# MAGIC    OR (  -- Changed records
# MAGIC      t.segment_id <> s.segment_id OR
# MAGIC      t.segment_name <> s.segment_name OR
# MAGIC      t.type_id <> s.type_id OR
# MAGIC      t.type_name <> s.type_name
# MAGIC    );

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
# MAGIC -- event_sk: Hash-based surrogate key (primary key)
# MAGIC -- venue_sk_fk: Nullable foreign key to dim_venue (hash surrogate)
# MAGIC -- attraction_sk_fk: Nullable foreign key to dim_attraction (hash surrogate)
# MAGIC -- classification_sk_fk: Nullable foreign key to dim_classification (hash surrogate)
# MAGIC -- date_sk_fk: Nullable foreign key to dim_date (surrogate key) for joins
# MAGIC -- event_date: Natural DATE field for readability and simple date filtering
# MAGIC -- event_datetime: Full TIMESTAMP for precise temporal queries
# MAGIC CREATE TABLE ticket_master.gold.fact_events (
# MAGIC   event_sk STRING NOT NULL,
# MAGIC   event_id STRING NOT NULL,
# MAGIC   event_name STRING,
# MAGIC   event_type STRING,
# MAGIC   date_sk_fk INT,
# MAGIC   venue_sk_fk STRING,
# MAGIC   attraction_sk_fk STRING,
# MAGIC   classification_sk_fk STRING,
# MAGIC   event_date DATE,
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
# MAGIC   CONSTRAINT fact_events_date_fk FOREIGN KEY (date_sk_fk)
# MAGIC     REFERENCES ticket_master.gold.dim_date(date_sk)
# MAGIC   -- Note: No FK constraints to SCD Type 2 dimensions (venue, attraction, classification)
# MAGIC   -- because their composite PKs (surrogate_key, valid_from) cannot be referenced
# MAGIC   -- by fact table which only stores surrogate_key. This is standard for SCD Type 2.
# MAGIC   -- Joins use: INNER JOIN dim_table d ON fact.dim_sk_fk = d.dim_sk AND d.is_current = TRUE
# MAGIC   --
# MAGIC   -- Date fields strategy:
# MAGIC   -- - date_sk_fk: FK to dim_date for joins and relationships
# MAGIC   -- - event_date: Natural DATE field for readability and simple date filtering
# MAGIC   -- - event_datetime: Full timestamp for precise temporal queries
# MAGIC )
# MAGIC CLUSTER BY (date_sk_fk, venue_sk_fk);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- MERGE fact_events for upsert capability (insert new, update existing)
# MAGIC MERGE INTO ticket_master.gold.fact_events AS t
# MAGIC USING (
# MAGIC   SELECT
# MAGIC     e.event_sk,
# MAGIC     ev.venue_sk_fk,
# MAGIC     ea.attraction_sk_fk,
# MAGIC     e.event_id,
# MAGIC     e.event_name,
# MAGIC     e.event_type,
# MAGIC     d.date_sk AS date_sk_fk,
# MAGIC     e.event_date,
# MAGIC     CAST(NULL AS STRING) AS classification_sk_fk, -- placeholder for future join
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
# MAGIC     ON e.event_sk = ev.event_sk_fk  -- Join on hash surrogate key
# MAGIC   LEFT JOIN ticket_master.silver.event_attractions ea
# MAGIC     ON e.event_sk = ea.event_sk_fk  -- Join on hash surrogate key
# MAGIC   LEFT JOIN ticket_master.gold.dim_date d
# MAGIC     ON e.event_date = d.date_value  -- Join on natural date to get surrogate key
# MAGIC ) AS s
# MAGIC -- Match on hash surrogate keys for deduplication
# MAGIC ON  t.event_sk          = s.event_sk
# MAGIC AND t.venue_sk_fk      <=> s.venue_sk_fk       -- null-safe equality
# MAGIC AND t.attraction_sk_fk <=> s.attraction_sk_fk  -- null-safe equality
# MAGIC 
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC   t.event_name            = s.event_name,
# MAGIC   t.event_type            = s.event_type,
# MAGIC   t.date_sk_fk            = s.date_sk_fk,
# MAGIC   t.event_date            = s.event_date,
# MAGIC   t.classification_sk_fk  = s.classification_sk_fk,
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
# MAGIC   event_sk,
# MAGIC   venue_sk_fk,
# MAGIC   attraction_sk_fk,
# MAGIC   event_id,
# MAGIC   event_name,
# MAGIC   event_type,
# MAGIC   date_sk_fk,
# MAGIC   event_date,
# MAGIC   classification_sk_fk,
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
# MAGIC   s.event_sk,
# MAGIC   s.venue_sk_fk,
# MAGIC   s.attraction_sk_fk,
# MAGIC   s.event_id,
# MAGIC   s.event_name,
# MAGIC   s.event_type,
# MAGIC   s.date_sk_fk,
# MAGIC   s.event_date,
# MAGIC   s.classification_sk_fk,
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
# MAGIC INNER JOIN ticket_master.gold.dim_date d ON f.date_sk_fk = d.date_sk
# MAGIC INNER JOIN ticket_master.gold.dim_venue v ON f.venue_sk_fk = v.venue_sk AND v.is_current = TRUE
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
# MAGIC INNER JOIN ticket_master.gold.dim_attraction a ON f.attraction_sk_fk = a.attraction_sk AND a.is_current = TRUE
# MAGIC INNER JOIN ticket_master.gold.dim_venue v ON f.venue_sk_fk = v.venue_sk AND v.is_current = TRUE
# MAGIC INNER JOIN ticket_master.gold.dim_date d ON f.date_sk_fk = d.date_sk
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
# MAGIC   COUNT(DISTINCT f.venue_sk_fk) as unique_venues,
# MAGIC   COUNT(DISTINCT f.attraction_sk_fk) as unique_attractions,
# MAGIC   AVG(f.price_min) as avg_min_price,
# MAGIC   AVG(f.price_max) as avg_max_price,
# MAGIC   COUNT(DISTINCT CASE WHEN d.is_weekend THEN f.event_id END) as weekend_events,
# MAGIC   COUNT(DISTINCT CASE WHEN NOT d.is_weekend THEN f.event_id END) as weekday_events
# MAGIC FROM ticket_master.gold.fact_events f
# MAGIC INNER JOIN ticket_master.gold.dim_date d ON f.date_sk_fk = d.date_sk
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
    "dim_date", "dim_classification"
]

for table in gold_tables:
    try:
        count = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.{table}").count()
        print(f"ticket_master.gold.{table}: {count:,} records")
    except:
        print(f"ticket_master.gold.{table}: Not found")

# COMMAND ----------
