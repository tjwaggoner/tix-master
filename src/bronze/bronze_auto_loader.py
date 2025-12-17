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
    current_timestamp, col, to_date,
    explode, when, lit
)
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Configuration (use these values directly)
CATALOG = "ticket_master"
BRONZE_SCHEMA = "bronze"

print(f"Using: {CATALOG}.{BRONZE_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set Unity Catalog Context

# COMMAND ----------

# CRITICAL: Set the current catalog to avoid Hive Metastore errors
spark.sql(f"USE CATALOG {CATALOG}")
print(f"✓ Using catalog: {spark.catalog.currentCatalog()}")

# COMMAND ----------

# Volume and checkpoint configuration
VOLUME_NAME = "raw_data"
VOLUME_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{VOLUME_NAME}"
# IMPORTANT: Use Unity Catalog volume for checkpoints (DBFS root /tmp is disabled)
CHECKPOINT_BASE = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{VOLUME_NAME}/_checkpoints"

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
# MAGIC ## Setup Bronze Layer

# COMMAND ----------

# Use the catalog and create schema/volume if needed
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}.{VOLUME_NAME}")

print(f"✓ Bronze layer ready: {CATALOG}.{BRONZE_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto Loader Streaming Functions

# COMMAND ----------

def create_bronze_stream(entity_config):
    """
    Create an Auto Loader streaming query for a given entity with deduplication

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

    # Read JSON files using Auto Loader (reads recursively from subdirectories)
    # Ticketmaster API returns events with varying fields (e.g., sales.presales may or may not exist)
    # Using addNewColumns mode: fails on new fields, then auto-adds them on retry
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # Auto-evolve schema
        .option("cloudFiles.useIncrementalListing", "auto")  # Optimized for directory listing
        .option("recursiveFileLookup", "true")  # Scan subdirectories (YYYY/MM/DD structure)
        .option("multiLine", "true")  # Handle multi-line JSON
        .load(source_path)
    )

    # Add metadata columns (using Unity Catalog metadata)
    df_with_metadata = (
        df
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))  # Unity Catalog compatible
        .withColumn("_ingestion_date", to_date(current_timestamp()))
    )

    # Write to Bronze Delta table with streaming (append mode for raw data)
    # Bronze layer keeps all raw data; deduplication happens in Silver layer
    query = (
        df_with_metadata.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")  # Allow schema evolution
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

print("Starting Auto Loader streams...")
for entity_config in ENTITIES:
    try:
        query = create_bronze_stream(entity_config)
        streaming_queries.append(query)
        print(f"  ✓ {entity_config['name']}")
    except Exception as e:
        print(f"  ✗ {entity_config['name']}: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wait for Completion

# COMMAND ----------

# Wait for all streams to complete
for query in streaming_queries:
    query.awaitTermination()

print(f"\n✓ Bronze layer processing complete!")
print(f"✓ Loaded {len(streaming_queries)} entities to {CATALOG}.{BRONZE_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Bronze Tables

# COMMAND ----------

# Show all bronze tables
print(f"\nTables in {CATALOG}.{BRONZE_SCHEMA}:")
try:
    spark.sql(f"SHOW TABLES IN {CATALOG}.{BRONZE_SCHEMA}").show()
except Exception as e:
    print(f"  Schema not yet created: {e}")

# COMMAND ----------

# Check record counts for each table
print("\nRecord counts:")
for entity_config in ENTITIES:
    table_name = entity_config["table_name"]
    try:
        if spark.catalog.tableExists(table_name):
            count = spark.table(table_name).count()
            print(f"  ✓ {table_name}: {count:,} records")
        else:
            print(f"  - {table_name}: Not yet created (no data in volume)")
    except Exception as e:
        print(f"  - {table_name}: Not accessible")

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

print("\n✓ Bronze layer ready for downstream processing!")
