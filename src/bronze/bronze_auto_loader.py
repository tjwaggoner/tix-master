# Databricks notebook source
"""
Bronze Layer - Auto Loader Streaming

This notebook sets up Auto Loader streaming from Unity Catalog Volumes to Bronze Delta tables.
It reads raw JSON files and writes them to structured Delta tables with minimal transformation.
"""

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze Layer: Auto Loader Setup
# MAGIC
# MAGIC This notebook implements the Bronze layer using Auto Loader (cloudFiles) to stream
# MAGIC raw JSON data from Unity Catalog Volumes into Bronze Delta tables.
# MAGIC
# MAGIC **Features:**
# MAGIC - Auto schema inference and evolution
# MAGIC - Incremental processing with checkpointing
# MAGIC - Ingestion metadata (timestamp, source file)
# MAGIC - Partitioning by ingestion date

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp, input_file_name, col, to_date,
    explode, when, lit
)
from pyspark.sql.types import *
import yaml

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Configuration
CATALOG = "ticketmaster"
BRONZE_SCHEMA = "bronze"
VOLUME_NAME = "raw_data"
VOLUME_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{VOLUME_NAME}"
CHECKPOINT_BASE = "/tmp/checkpoints/bronze"

# Entity configurations
ENTITIES = [
    {
        "name": "events",
        "source_path": f"{VOLUME_PATH}/events",
        "table_name": f"{CATALOG}.{BRONZE_SCHEMA}.events_raw",
        "checkpoint_path": f"{CHECKPOINT_BASE}/events"
    },
    {
        "name": "venues",
        "source_path": f"{VOLUME_PATH}/venues",
        "table_name": f"{CATALOG}.{BRONZE_SCHEMA}.venues_raw",
        "checkpoint_path": f"{CHECKPOINT_BASE}/venues"
    },
    {
        "name": "attractions",
        "source_path": f"{VOLUME_PATH}/attractions",
        "table_name": f"{CATALOG}.{BRONZE_SCHEMA}.attractions_raw",
        "checkpoint_path": f"{CHECKPOINT_BASE}/attractions"
    },
    {
        "name": "classifications",
        "source_path": f"{VOLUME_PATH}/classifications",
        "table_name": f"{CATALOG}.{BRONZE_SCHEMA}.classifications_raw",
        "checkpoint_path": f"{CHECKPOINT_BASE}/classifications"
    }
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unity Catalog Setup

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create catalog if not exists
# MAGIC CREATE CATALOG IF NOT EXISTS ticketmaster;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create bronze schema
# MAGIC CREATE SCHEMA IF NOT EXISTS ticketmaster.bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create volume for raw data
# MAGIC CREATE VOLUME IF NOT EXISTS ticketmaster.bronze.raw_data;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto Loader Streaming Functions

# COMMAND ----------

def create_bronze_stream(entity_config):
    """
    Create an Auto Loader streaming query for a given entity

    Args:
        entity_config: Dictionary with entity configuration

    Returns:
        StreamingQuery object
    """
    source_path = entity_config["source_path"]
    table_name = entity_config["table_name"]
    checkpoint_path = entity_config["checkpoint_path"]
    entity_name = entity_config["name"]

    print(f"Setting up Auto Loader stream for {entity_name}")
    print(f"  Source: {source_path}")
    print(f"  Target: {table_name}")

    # Read JSON files using Auto Loader
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("multiLine", "true")  # Handle multi-line JSON
        .load(source_path)
    )

    # Add metadata columns
    df_with_metadata = (
        df
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", input_file_name())
        .withColumn("_ingestion_date", to_date(current_timestamp()))
    )

    # Write to Bronze Delta table with streaming
    # Using ingestion time clustering (automatic in DBR 11.3+) instead of partitioning
    query = (
        df_with_metadata.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)  # Process all available files then stop
        .table(table_name)
    )

    return query

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Streaming Ingestion

# COMMAND ----------

# Create and start streaming queries for all entities
streaming_queries = []

for entity_config in ENTITIES:
    try:
        query = create_bronze_stream(entity_config)
        streaming_queries.append(query)
        print(f"✓ Started streaming for {entity_config['name']}")
    except Exception as e:
        print(f"✗ Error setting up stream for {entity_config['name']}: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wait for Completion (Batch Mode)

# COMMAND ----------

# Wait for all streams to complete (when using trigger.availableNow)
for query in streaming_queries:
    query.awaitTermination()
    print(f"Stream {query.name} completed")

print("All Bronze layer streams completed successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Bronze Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show all bronze tables
# MAGIC SHOW TABLES IN ticketmaster.bronze;

# COMMAND ----------

# Check record counts for each table
for entity_config in ENTITIES:
    table_name = entity_config["table_name"]
    count = spark.table(table_name).count()
    print(f"{table_name}: {count:,} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Data Inspection

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sample events data
# MAGIC SELECT * FROM ticketmaster.bronze.events_raw LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sample venues data
# MAGIC SELECT * FROM ticketmaster.bronze.venues_raw LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Continuous Streaming Mode (Optional)
# MAGIC
# MAGIC For production, you may want continuous streaming instead of batch processing.
# MAGIC Use the function below instead of trigger(availableNow=True).

# COMMAND ----------

def create_continuous_bronze_stream(entity_config):
    """
    Create a continuously running Auto Loader stream
    """
    source_path = entity_config["source_path"]
    table_name = entity_config["table_name"]
    checkpoint_path = entity_config["checkpoint_path"]

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("multiLine", "true")
        .load(source_path)
    )

    df_with_metadata = (
        df
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", input_file_name())
        .withColumn("_ingestion_date", to_date(current_timestamp()))
    )

    # Use processingTime trigger for micro-batch streaming
    query = (
        df_with_metadata.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .partitionBy("_ingestion_date")
        .trigger(processingTime="5 minutes")  # Check for new files every 5 minutes
        .table(table_name)
    )

    return query

# COMMAND ----------

# Example: Start continuous streaming (uncomment to use)
# continuous_queries = []
# for entity_config in ENTITIES:
#     query = create_continuous_bronze_stream(entity_config)
#     continuous_queries.append(query)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitoring and Maintenance

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check table properties
# MAGIC DESCRIBE EXTENDED ticketmaster.bronze.events_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View table history
# MAGIC DESCRIBE HISTORY ticketmaster.bronze.events_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Optimize tables (run periodically)
# MAGIC OPTIMIZE ticketmaster.bronze.events_raw;
# MAGIC OPTIMIZE ticketmaster.bronze.venues_raw;
# MAGIC OPTIMIZE ticketmaster.bronze.attractions_raw;
# MAGIC OPTIMIZE ticketmaster.bronze.classifications_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Vacuum old files (run periodically, e.g., weekly)
# MAGIC VACUUM ticketmaster.bronze.events_raw RETAIN 168 HOURS;  -- 7 days
