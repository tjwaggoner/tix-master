# Databricks notebook source
"""
Silver Layer - Normalized Relational Tables with Incremental Streaming

This notebook transforms Bronze raw data into normalized Silver tables with:
- Proper data types and validation
- Incremental processing using Structured Streaming
- Deduplication via MERGE operations
- Primary Key and Foreign Key constraints
- Entity-Relationship Design (ERD)
"""

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Layer: Incremental Streaming Transformations
# MAGIC
# MAGIC This notebook implements the Silver layer using **Structured Streaming** to incrementally
# MAGIC process only new data from Bronze tables.
# MAGIC
# MAGIC **Key Features:**
# MAGIC 1. Incremental processing - only new Bronze data is transformed
# MAGIC 2. MERGE operations - automatic deduplication and upserts
# MAGIC 3. Watermarking - handles late-arriving data
# MAGIC 4. Checkpointing - fault-tolerant processing
# MAGIC 5. PK/FK constraints for query optimization
# MAGIC 6. ERD visualization in Unity Catalog

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, explode_outer, when, lit, coalesce,
    struct, array, concat_ws, md5, sha2, monotonically_increasing_id,
    row_number, max as spark_max, first, current_timestamp
)
from pyspark.sql.window import Window
from pyspark.sql.types import *
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Configuration
CATALOG = "ticket_master"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

# Checkpoint location for streaming
VOLUME_NAME = "raw_data"
CHECKPOINT_BASE = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{VOLUME_NAME}/_checkpoints/silver"

# Set catalog context to avoid Hive Metastore errors
spark.sql(f"USE CATALOG {CATALOG}")
print(f"✓ Using catalog: {spark.catalog.currentCatalog()}")

# Create Silver schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")
print(f"✓ Silver schema ready: {CATALOG}.{SILVER_SCHEMA}")

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

def merge_upsert(microBatchDF, batch_id, target_table, merge_keys, update_columns=None):
    """
    Generic MERGE operation for streaming upserts with deduplication.
    
    Args:
        microBatchDF: Micro-batch DataFrame from streaming
        batch_id: Batch ID from streaming
        target_table: Full table name (catalog.schema.table)
        merge_keys: List of columns to match on (typically primary keys)
        update_columns: List of columns to update (None = all columns)
    """
    
    # Deduplicate within the micro-batch using the merge keys
    # Keep the most recent record based on _ingestion_timestamp
    window_spec = Window.partitionBy(*merge_keys).orderBy(col("_ingestion_timestamp").desc())
    deduped_df = microBatchDF.withColumn("rn", row_number().over(window_spec)) \
                              .filter(col("rn") == 1) \
                              .drop("rn")
    
    # Check if target table exists
    if not spark.catalog.tableExists(target_table):
        # First run - create table with the data
        print(f"  Creating initial table: {target_table}")
        deduped_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
        return
    
    # Build merge condition
    merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in merge_keys])
    
    # Get all columns for update (excluding merge keys)
    if update_columns is None:
        update_columns = [c for c in deduped_df.columns if c not in merge_keys]
    
    # Build update/insert dict
    update_dict = {col_name: f"source.{col_name}" for col_name in update_columns + merge_keys}
    
    # Perform MERGE
    deltaTable = DeltaTable.forName(spark, target_table)
    
    (deltaTable.alias("target")
        .merge(
            deduped_df.alias("source"),
            merge_condition
        )
        .whenMatchedUpdate(set=update_dict)
        .whenNotMatchedInsert(values=update_dict)
        .execute()
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Bronze Tables Exist

# COMMAND ----------

# Verify bronze tables exist before processing
required_tables = ['events_raw', 'venues_raw', 'attractions_raw', 'classifications_raw']
missing_tables = []

for table in required_tables:
    full_table_name = f"{CATALOG}.{BRONZE_SCHEMA}.{table}"
    if not spark.catalog.tableExists(full_table_name):
        missing_tables.append(full_table_name)
        print(f"⚠️  Missing: {full_table_name}")
    else:
        count = spark.table(full_table_name).count()
        print(f"✓ Found: {full_table_name} ({count:,} records)")

if missing_tables:
    error_msg = f"Bronze tables not found: {', '.join(missing_tables)}. Run ingestion and bronze loader first."
    print(f"\n❌ ERROR: {error_msg}")
    dbutils.notebook.exit(error_msg)

print("\n✓ All required bronze tables exist. Proceeding with silver transformations...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming Silver Table Creation - Venues Dimension

# COMMAND ----------

def create_silver_venues_stream():
    """
    Stream venues from Bronze to Silver with incremental processing
    """
    
    silver_table = f"{CATALOG}.{SILVER_SCHEMA}.venues"
    checkpoint_path = f"{CHECKPOINT_BASE}/venues"
    
    # Stream from Bronze
    bronze_stream = (
        spark.readStream
        .format("delta")
        .table(f"{CATALOG}.{BRONZE_SCHEMA}.venues_raw")
    )
    
    # Transform
    transformed_stream = (
        bronze_stream
        .select(
            col("id").alias("venue_id"),
            col("name").alias("venue_name"),
            col("type").alias("venue_type"),
            col("locale").alias("locale"),
            col("postalCode").alias("postal_code"),
            col("timezone").alias("timezone"),
            col("city.name").alias("city"),
            col("state.name").alias("state"),
            col("state.stateCode").alias("state_code"),
            col("country.name").alias("country"),
            col("country.countryCode").alias("country_code"),
            col("address.line1").alias("address_line1"),
            col("location.longitude").cast("double").alias("longitude"),
            col("location.latitude").cast("double").alias("latitude"),
            col("url").alias("venue_url"),
            col("markets").alias("markets"),  # Embed markets array as Variant
            col("_ingestion_timestamp")
        )
        .filter(col("venue_id").isNotNull())
        # Add surrogate key based on business attributes for deduplication
        .withColumn("venue_sk",
            md5(concat_ws("||",
                coalesce(col("venue_name"), lit("")),
                coalesce(col("latitude").cast("string"), lit("0")),
                coalesce(col("longitude").cast("string"), lit("0"))
            ))
        )
    )
    
    # Write with MERGE for deduplication on surrogate key
    query = (
        transformed_stream.writeStream
        .format("delta")
        .outputMode("update")
        .option("checkpointLocation", checkpoint_path)
        .foreachBatch(lambda df, batch_id: merge_upsert(
            df, batch_id, silver_table,
            merge_keys=["venue_sk"]  # Deduplicate on surrogate key, not API ID
        ))
        .trigger(availableNow=True)
        .start()
    )
    
    return query

# Start the stream
print("Starting venues stream...")
venues_query = create_silver_venues_stream()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming Silver Table Creation - Attractions/Teams Dimension

# COMMAND ----------

def create_silver_attractions_stream():
    """
    Stream attractions from Bronze to Silver with incremental processing
    """
    
    silver_table = f"{CATALOG}.{SILVER_SCHEMA}.attractions"
    checkpoint_path = f"{CHECKPOINT_BASE}/attractions"
    
    # Stream from Bronze
    bronze_stream = (
        spark.readStream
        .format("delta")
        .table(f"{CATALOG}.{BRONZE_SCHEMA}.attractions_raw")
    )
    
    # Transform
    transformed_stream = (
        bronze_stream
        .select(
            col("id").alias("attraction_id"),
            col("name").alias("attraction_name"),
            col("type").alias("attraction_type"),
            col("locale").alias("locale"),
            col("url").alias("attraction_url"),
            col("test").cast("boolean").alias("is_test"),
            explode_outer(col("classifications")).alias("classification_exploded"),
            col("_ingestion_timestamp")
        )
        .select(
            "attraction_id",
            "attraction_name",
            "attraction_type",
            "locale",
            "attraction_url",
            "is_test",
            col("classification_exploded.segment.id").alias("segment_id"),
            col("classification_exploded.segment.name").alias("segment_name"),
            col("classification_exploded.genre.id").alias("genre_id"),
            col("classification_exploded.genre.name").alias("genre_name"),
            "_ingestion_timestamp"
        )
        .filter(col("attraction_id").isNotNull())
        # Add surrogate key based on business attributes for deduplication
        .withColumn("attraction_sk",
            md5(concat_ws("||",
                coalesce(col("attraction_name"), lit("")),
                coalesce(col("segment_name"), lit("NONE"))
            ))
        )
    )
    
    # Write with MERGE for deduplication on surrogate key
    query = (
        transformed_stream.writeStream
        .format("delta")
        .outputMode("update")
        .option("checkpointLocation", checkpoint_path)
        .foreachBatch(lambda df, batch_id: merge_upsert(
            df, batch_id, silver_table,
            merge_keys=["attraction_sk"]  # Deduplicate on surrogate key, not API ID
        ))
        .trigger(availableNow=True)
        .start()
    )
    
    return query

# Start the stream
print("Starting attractions stream...")
attractions_query = create_silver_attractions_stream()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming Silver Table Creation - Classifications Dimension

# COMMAND ----------

def create_silver_classifications_stream():
    """
    Stream classifications from Bronze to Silver with incremental processing
    """
    
    silver_table = f"{CATALOG}.{SILVER_SCHEMA}.classifications"
    checkpoint_path = f"{CHECKPOINT_BASE}/classifications"
    
    # Stream from Bronze
    bronze_stream = (
        spark.readStream
        .format("delta")
        .table(f"{CATALOG}.{BRONZE_SCHEMA}.classifications_raw")
    )
    
    # Get available columns from the schema
    available_cols = bronze_stream.schema.fieldNames()
    
    # Build select expression dynamically
    select_exprs = []
    
    if "segment" in available_cols:
        select_exprs.extend([
            col("segment.id").alias("segment_id"),
            col("segment.name").alias("segment_name")
        ])
    
    if "genre" in available_cols:
        select_exprs.extend([
            col("genre.id").alias("genre_id"),
            col("genre.name").alias("genre_name")
        ])
    
    if "subGenre" in available_cols:
        select_exprs.extend([
            col("subGenre.id").alias("subgenre_id"),
            col("subGenre.name").alias("subgenre_name")
        ])
    
    if "type" in available_cols:
        select_exprs.extend([
            col("type.id").alias("type_id"),
            col("type.name").alias("type_name")
        ])
    
    if "subType" in available_cols:
        select_exprs.extend([
            col("subType.id").alias("subtype_id"),
            col("subType.name").alias("subtype_name")
        ])
    
    if "family" in available_cols:
        select_exprs.append(col("family").cast("boolean").alias("is_family"))
    
    if "_ingestion_timestamp" in available_cols:
        select_exprs.append(col("_ingestion_timestamp"))
    
    # Transform
    df_selected = bronze_stream.select(*select_exprs)
    
    # Build composite key
    available_key_cols = df_selected.schema.fieldNames()
    key_parts = []
    
    for key_col in ["segment_id", "genre_id", "subgenre_id", "type_id", "subtype_id"]:
        if key_col in available_key_cols:
            key_parts.append(coalesce(col(key_col), lit("")))
    
    transformed_stream = (
        df_selected
        .withColumn("classification_id", sha2(concat_ws("_", *key_parts), 256))
        .filter(col("classification_id").isNotNull())
    )
    
    # Write with MERGE for deduplication
    query = (
        transformed_stream.writeStream
        .format("delta")
        .outputMode("update")
        .option("checkpointLocation", checkpoint_path)
        .foreachBatch(lambda df, batch_id: merge_upsert(
            df, batch_id, silver_table, 
            merge_keys=["classification_id"]
        ))
        .trigger(availableNow=True)
        .start()
    )
    
    return query

# Start the stream
print("Starting classifications stream...")
classifications_query = create_silver_classifications_stream()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Markets are embedded in dim_venue as Variant
# MAGIC Markets are no longer a separate dimension table.
# MAGIC They are included as a VARIANT array column in dim_venue.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wait for Dimension Tables to Complete

# COMMAND ----------

# Wait for all dimension streams to complete before creating fact tables
print("\nWaiting for dimension table streams to complete...")

venues_query.awaitTermination()
print("  ✓ Venues completed")

attractions_query.awaitTermination()
print("  ✓ Attractions completed")

classifications_query.awaitTermination()
print("  ✓ Classifications completed")

markets_query.awaitTermination()
print("  ✓ Markets completed")

print("\n✓ All dimension tables loaded!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Primary Keys to Dimension Tables

# COMMAND ----------

# Add constraints to dimension tables after initial load
print("\nAdding primary key constraints to dimension tables...")

add_primary_key_if_not_exists(f"{CATALOG}.{SILVER_SCHEMA}.venues", "venues_pk", ["venue_id"])
add_primary_key_if_not_exists(f"{CATALOG}.{SILVER_SCHEMA}.attractions", "attractions_pk", ["attraction_id"])
add_primary_key_if_not_exists(f"{CATALOG}.{SILVER_SCHEMA}.classifications", "classifications_pk", ["classification_id"])

print("✓ Primary key constraints added")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming Silver Table Creation - Events Fact Table

# COMMAND ----------

def create_silver_events_stream():
    """
    Stream events from Bronze to Silver (fact table)
    """
    
    silver_table = f"{CATALOG}.{SILVER_SCHEMA}.events"
    checkpoint_path = f"{CHECKPOINT_BASE}/events"
    
    # Stream from Bronze
    bronze_stream = (
        spark.readStream
        .format("delta")
        .table(f"{CATALOG}.{BRONZE_SCHEMA}.events_raw")
    )
    
    # Transform
    transformed_stream = (
        bronze_stream
        .select(
            col("id").alias("event_id"),
            col("name").alias("event_name"),
            col("type").alias("event_type"),
            col("url").alias("event_url"),
            col("locale").alias("locale"),
            col("info").alias("event_info"),
            col("pleaseNote").alias("please_note"),
            col("priceRanges")[0]["min"].cast("double").alias("price_min"),
            col("priceRanges")[0]["max"].cast("double").alias("price_max"),
            col("priceRanges")[0]["currency"].alias("price_currency"),
            col("dates.start.localDate").cast("date").alias("event_date"),
            col("dates.start.localTime").alias("event_time"),
            col("dates.start.dateTime").cast("timestamp").alias("event_datetime"),
            col("dates.timezone").alias("event_timezone"),
            col("dates.status.code").alias("status_code"),
            col("sales.public.startDateTime").cast("timestamp").alias("sales_start_datetime"),
            col("sales.public.endDateTime").cast("timestamp").alias("sales_end_datetime"),
            col("test").cast("boolean").alias("is_test"),
            col("classifications")[0]["segment"]["id"].alias("segment_id"),
            col("classifications")[0]["genre"]["id"].alias("genre_id"),
            col("_ingestion_timestamp")
        )
        .filter(col("event_id").isNotNull())
        # Add surrogate key based on business attributes for deduplication
        .withColumn("event_sk",
            md5(concat_ws("||",
                coalesce(col("event_name"), lit("")),
                coalesce(col("event_datetime").cast("string"), lit("1970-01-01"))
            ))
        )
    )
    
    # Write with MERGE for deduplication on surrogate key
    query = (
        transformed_stream.writeStream
        .format("delta")
        .outputMode("update")
        .option("checkpointLocation", checkpoint_path)
        .foreachBatch(lambda df, batch_id: merge_upsert(
            df, batch_id, silver_table,
            merge_keys=["event_sk"]  # Deduplicate on surrogate key, not API ID
        ))
        .trigger(availableNow=True)
        .start()
    )
    
    return query

# Start the stream
print("Starting events stream...")
events_query = create_silver_events_stream()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming Silver Table Creation - Event-Venue Bridge Table

# COMMAND ----------

def create_silver_event_venues_stream():
    """
    Stream event-venue relationships from Bronze to Silver
    """

    silver_table = f"{CATALOG}.{SILVER_SCHEMA}.event_venues"
    checkpoint_path = f"{CHECKPOINT_BASE}/event_venues"

    # Stream from Bronze
    bronze_stream = (
        spark.readStream
        .format("delta")
        .table(f"{CATALOG}.{BRONZE_SCHEMA}.events_raw")
    )

    # Transform
    transformed_stream = (
        bronze_stream
        .select(
            # Event fields for surrogate key
            col("id").alias("event_id"),
            col("name").alias("event_name"),
            col("dates.start.dateTime").cast("timestamp").alias("event_datetime"),
            # Venue fields
            explode_outer(col("_embedded.venues")).alias("venue"),
            col("_ingestion_timestamp")
        )
        .select(
            "event_id",
            "event_name",
            "event_datetime",
            col("venue.id").alias("venue_id"),
            col("venue.name").alias("venue_name"),
            col("venue.location.latitude").cast("double").alias("latitude"),
            col("venue.location.longitude").cast("double").alias("longitude"),
            col("_ingestion_timestamp")
        )
        .filter(col("event_id").isNotNull() & col("venue_id").isNotNull())
        # Add event surrogate key (foreign key reference)
        .withColumn("event_sk_fk",
            md5(concat_ws("||",
                coalesce(col("event_name"), lit("")),
                coalesce(col("event_datetime").cast("string"), lit("1970-01-01"))
            ))
        )
        # Add venue surrogate key (foreign key reference)
        .withColumn("venue_sk_fk",
            md5(concat_ws("||",
                coalesce(col("venue_name"), lit("")),
                coalesce(col("latitude").cast("string"), lit("0")),
                coalesce(col("longitude").cast("string"), lit("0"))
            ))
        )
        # Keep only the keys we need in the bridge table
        .select("event_id", "venue_id", "event_sk_fk", "venue_sk_fk", "_ingestion_timestamp")
    )

    # Write with MERGE for deduplication on surrogate keys
    query = (
        transformed_stream.writeStream
        .format("delta")
        .outputMode("update")
        .option("checkpointLocation", checkpoint_path)
        .foreachBatch(lambda df, batch_id: merge_upsert(
            df, batch_id, silver_table,
            merge_keys=["event_sk_fk", "venue_sk_fk"]  # Deduplicate on surrogate keys
        ))
        .trigger(availableNow=True)
        .start()
    )

    return query

# Start the stream
print("Starting event_venues stream...")
event_venues_query = create_silver_event_venues_stream()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming Silver Table Creation - Event-Attraction Bridge Table

# COMMAND ----------

def create_silver_event_attractions_stream():
    """
    Stream event-attraction relationships from Bronze to Silver
    """

    silver_table = f"{CATALOG}.{SILVER_SCHEMA}.event_attractions"
    checkpoint_path = f"{CHECKPOINT_BASE}/event_attractions"

    # Stream from Bronze
    bronze_stream = (
        spark.readStream
        .format("delta")
        .table(f"{CATALOG}.{BRONZE_SCHEMA}.events_raw")
    )

    # Transform
    transformed_stream = (
        bronze_stream
        .select(
            # Event fields for surrogate key
            col("id").alias("event_id"),
            col("name").alias("event_name"),
            col("dates.start.dateTime").cast("timestamp").alias("event_datetime"),
            # Classification for attraction surrogate key
            col("classifications")[0]["segment"]["name"].alias("segment_name"),
            # Attraction fields
            explode_outer(col("_embedded.attractions")).alias("attraction"),
            col("_ingestion_timestamp")
        )
        .select(
            "event_id",
            "event_name",
            "event_datetime",
            col("attraction.id").alias("attraction_id"),
            col("attraction.name").alias("attraction_name"),
            "segment_name",
            col("_ingestion_timestamp")
        )
        .filter(col("event_id").isNotNull() & col("attraction_id").isNotNull())
        # Add event surrogate key (foreign key reference)
        .withColumn("event_sk_fk",
            md5(concat_ws("||",
                coalesce(col("event_name"), lit("")),
                coalesce(col("event_datetime").cast("string"), lit("1970-01-01"))
            ))
        )
        # Add attraction surrogate key (foreign key reference)
        .withColumn("attraction_sk_fk",
            md5(concat_ws("||",
                coalesce(col("attraction_name"), lit("")),
                coalesce(col("segment_name"), lit("NONE"))
            ))
        )
        # Keep only the keys we need in the bridge table
        .select("event_id", "attraction_id", "event_sk_fk", "attraction_sk_fk", "_ingestion_timestamp")
    )

    # Write with MERGE for deduplication on surrogate keys
    query = (
        transformed_stream.writeStream
        .format("delta")
        .outputMode("update")
        .option("checkpointLocation", checkpoint_path)
        .foreachBatch(lambda df, batch_id: merge_upsert(
            df, batch_id, silver_table,
            merge_keys=["event_sk_fk", "attraction_sk_fk"]  # Deduplicate on surrogate keys
        ))
        .trigger(availableNow=True)
        .start()
    )

    return query

# Start the stream
print("Starting event_attractions stream...")
event_attractions_query = create_silver_event_attractions_stream()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wait for Fact/Bridge Tables to Complete

# COMMAND ----------

# Wait for all fact/bridge streams to complete
print("\nWaiting for fact/bridge table streams to complete...")

events_query.awaitTermination()
print("  ✓ Events completed")

event_venues_query.awaitTermination()
print("  ✓ Event-Venues completed")

event_attractions_query.awaitTermination()
print("  ✓ Event-Attractions completed")

print("\n✓ All fact and bridge tables loaded!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Constraints to Fact/Bridge Tables

# COMMAND ----------

# Add constraints after initial load
print("\nAdding constraints to fact and bridge tables...")

# Events primary key
add_primary_key_if_not_exists(f"{CATALOG}.{SILVER_SCHEMA}.events", "events_pk", ["event_id"])

# Enable liquid clustering on events
spark.sql(f"""
    ALTER TABLE {CATALOG}.{SILVER_SCHEMA}.events
    CLUSTER BY (event_date, status_code)
""")
print("  ✓ Enabled liquid clustering on events")

# Event-Venues constraints
add_primary_key_if_not_exists(f"{CATALOG}.{SILVER_SCHEMA}.event_venues", "event_venues_pk", ["event_id", "venue_id"])

add_foreign_key_if_not_exists(
    table_name=f"{CATALOG}.{SILVER_SCHEMA}.event_venues",
    constraint_name="event_venues_event_fk",
    fk_columns=["event_id"],
    reference_table=f"{CATALOG}.{SILVER_SCHEMA}.events",
    reference_columns=["event_id"]
)

add_foreign_key_if_not_exists(
    table_name=f"{CATALOG}.{SILVER_SCHEMA}.event_venues",
    constraint_name="event_venues_venue_fk",
    fk_columns=["venue_id"],
    reference_table=f"{CATALOG}.{SILVER_SCHEMA}.venues",
    reference_columns=["venue_id"]
)

# Event-Attractions constraints
add_primary_key_if_not_exists(f"{CATALOG}.{SILVER_SCHEMA}.event_attractions", "event_attractions_pk", ["event_id", "attraction_id"])

add_foreign_key_if_not_exists(
    table_name=f"{CATALOG}.{SILVER_SCHEMA}.event_attractions",
    constraint_name="event_attractions_event_fk",
    fk_columns=["event_id"],
    reference_table=f"{CATALOG}.{SILVER_SCHEMA}.events",
    reference_columns=["event_id"]
)

add_foreign_key_if_not_exists(
    table_name=f"{CATALOG}.{SILVER_SCHEMA}.event_attractions",
    constraint_name="event_attractions_attraction_fk",
    fk_columns=["attraction_id"],
    reference_table=f"{CATALOG}.{SILVER_SCHEMA}.attractions",
    reference_columns=["attraction_id"]
)

print("✓ All constraints added successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Silver Layer

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show all silver tables
# MAGIC SHOW TABLES IN ticket_master.silver;

# COMMAND ----------

# Display record counts
silver_tables = [
    "events", "venues", "attractions", "classifications",
    "event_venues", "event_attractions"
]

print("\n=== Silver Layer Summary ===")
for table in silver_tables:
    count = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.{table}").count()
    print(f"  {CATALOG}.{SILVER_SCHEMA}.{table}: {count:,} records")

print("\n✓ Silver layer streaming transformations complete!")
print("✓ All tables use incremental processing from Bronze")
print("✓ Future runs will only process new Bronze data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View ERD in Unity Catalog
# MAGIC
# MAGIC Navigate to Unity Catalog UI:
# MAGIC 1. Open Catalog Explorer
# MAGIC 2. Select ticket_master catalog
# MAGIC 3. Select silver schema
# MAGIC 4. Click "Lineage" tab to see ERD visualization
# MAGIC
# MAGIC The PK/FK constraints enable Unity Catalog to automatically generate the ERD!
