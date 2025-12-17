# Databricks notebook source
"""
Silver Layer - Normalized Relational Tables

This notebook transforms Bronze raw data into normalized Silver tables with:
- Proper data types and validation
- Deduplication
- Primary Key and Foreign Key constraints
- Entity-Relationship Design (ERD)
"""

# COMMAND ----------

# MAGIC %md
# MAGIC # Silver Layer: Normalized Tables with PK/FK Constraints
# MAGIC
# MAGIC This notebook implements the Silver layer by:
# MAGIC 1. Normalizing nested JSON from Bronze
# MAGIC 2. Creating dimension tables (venues, attractions, classifications, markets, teams)
# MAGIC 3. Creating fact/bridge tables (events, event_venues, event_attractions)
# MAGIC 4. Applying PK/FK constraints for query optimization
# MAGIC 5. Enabling ERD visualization in Unity Catalog

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, explode_outer, when, lit, coalesce,
    struct, array, concat_ws, md5, sha2, monotonically_increasing_id,
    row_number, max as spark_max, first
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
# MAGIC ## Silver Table Creation - Venues Dimension

# COMMAND ----------

def create_silver_venues():
    """
    Extract and transform venues from Bronze to Silver
    Creates a normalized venues dimension table
    """

    # Read from Bronze
    bronze_venues = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.venues_raw")

    # Transform and normalize
    silver_venues = (
        bronze_venues
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
            col("_ingestion_timestamp")
        )
        .dropDuplicates(["venue_id"])
        .filter(col("venue_id").isNotNull())
    )

    # Write to Silver with MERGE to handle updates
    silver_table = f"{CATALOG}.{SILVER_SCHEMA}.venues"

    (
        silver_venues.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(silver_table)
    )

    # Add primary key constraint (if not exists)
    add_primary_key_if_not_exists(silver_table, "venues_pk", ["venue_id"])

    print(f"✓ Created {silver_table} with {silver_venues.count():,} records")

# COMMAND ----------

create_silver_venues()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Table Creation - Attractions/Teams Dimension

# COMMAND ----------

def create_silver_attractions():
    """
    Extract and transform attractions/teams from Bronze to Silver
    """

    bronze_attractions = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.attractions_raw")

    silver_attractions = (
        bronze_attractions
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
        .dropDuplicates(["attraction_id"])
        .filter(col("attraction_id").isNotNull())
    )

    silver_table = f"{CATALOG}.{SILVER_SCHEMA}.attractions"

    (
        silver_attractions.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(silver_table)
    )

    # Add primary key constraint (if not exists)
    add_primary_key_if_not_exists(silver_table, "attractions_pk", ["attraction_id"])

    print(f"✓ Created {silver_table} with {silver_attractions.count():,} records")

# COMMAND ----------

create_silver_attractions()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Table Creation - Classifications Dimension

# COMMAND ----------

def create_silver_classifications():
    """
    Extract classifications (segment, genre, subgenre) hierarchy
    """

    bronze_classifications = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.classifications_raw")
    
    # First, let's inspect the schema
    print("  Bronze classifications schema:")
    bronze_classifications.printSchema()
    print(f"  Bronze classifications sample (first row):")
    bronze_classifications.show(1, truncate=False, vertical=True)
    
    # Get available columns
    available_cols = bronze_classifications.columns
    print(f"  Available columns: {available_cols}")
    
    # Build select expression dynamically based on available columns
    select_exprs = []
    
    # Check for nested segment structure
    if "segment" in available_cols:
        select_exprs.extend([
            col("segment.id").alias("segment_id"),
            col("segment.name").alias("segment_name")
        ])
    
    # Check for nested genre structure  
    if "genre" in available_cols:
        select_exprs.extend([
            col("genre.id").alias("genre_id"),
            col("genre.name").alias("genre_name")
        ])
    
    # Check for nested subGenre structure
    if "subGenre" in available_cols:
        select_exprs.extend([
            col("subGenre.id").alias("subgenre_id"),
            col("subGenre.name").alias("subgenre_name")
        ])
    
    # Check for nested type structure
    if "type" in available_cols:
        select_exprs.extend([
            col("type.id").alias("type_id"),
            col("type.name").alias("type_name")
        ])
    
    # Check for nested subType structure
    if "subType" in available_cols:
        select_exprs.extend([
            col("subType.id").alias("subtype_id"),
            col("subType.name").alias("subtype_name")
        ])
    
    # Check for family field
    if "family" in available_cols:
        select_exprs.append(col("family").cast("boolean").alias("is_family"))
    
    # Always include ingestion timestamp
    if "_ingestion_timestamp" in available_cols:
        select_exprs.append(col("_ingestion_timestamp"))
    
    if not select_exprs:
        print("  ⚠️  No recognizable classification columns found!")
        print("  Available columns:", available_cols)
        raise ValueError("Cannot extract classifications - schema doesn't match expected structure")
    
    # First, create the base dataframe with selected columns
    df_selected = bronze_classifications.select(*select_exprs)
    
    # Build the composite key based on available columns in the dataframe
    available_key_cols = df_selected.columns
    key_parts = []
    
    for key_col in ["segment_id", "genre_id", "subgenre_id", "type_id", "subtype_id"]:
        if key_col in available_key_cols:
            key_parts.append(coalesce(col(key_col), lit("")))
    
    if not key_parts:
        raise ValueError("No valid key columns found for classification_id")
    
    print(f"  Using columns for classification_id: {[k for k in ['segment_id', 'genre_id', 'subgenre_id', 'type_id', 'subtype_id'] if k in available_key_cols]}")
    
    silver_classifications = (
        df_selected
        # Create a composite key since classifications are hierarchical
        .withColumn("classification_id", sha2(concat_ws("_", *key_parts), 256))
        .dropDuplicates(["classification_id"])
        .filter(col("classification_id").isNotNull())
    )

    silver_table = f"{CATALOG}.{SILVER_SCHEMA}.classifications"

    (
        silver_classifications.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(silver_table)
    )

    # Add primary key constraint (if not exists)
    add_primary_key_if_not_exists(silver_table, "classifications_pk", ["classification_id"])

    row_count = spark.table(silver_table).count()
    print(f"✓ Created {silver_table} with {row_count:,} records")

# COMMAND ----------

create_silver_classifications()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Table Creation - Markets Dimension

# COMMAND ----------

def create_silver_markets():
    """
    Extract markets from events
    """

    bronze_events = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.events_raw")

    # Extract markets from embedded _embedded field
    silver_markets = (
        bronze_events
        .select(
            explode_outer(col("_embedded.venues")).alias("venue")
        )
        .select(
            explode_outer(col("venue.markets")).alias("market")
        )
        .select(
            col("market.id").alias("market_id"),
            col("market.name").alias("market_name")
        )
        .dropDuplicates(["market_id"])
        .filter(col("market_id").isNotNull())
    )

    silver_table = f"{CATALOG}.{SILVER_SCHEMA}.markets"

    (
        silver_markets.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(silver_table)
    )

    # Add primary key constraint (if not exists)
    add_primary_key_if_not_exists(silver_table, "markets_pk", ["market_id"])

    print(f"✓ Created {silver_table} with {silver_markets.count():,} records")

# COMMAND ----------

create_silver_markets()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Table Creation - Events Fact Table

# COMMAND ----------

def create_silver_events():
    """
    Create the main events fact table with foreign keys to dimensions
    """

    bronze_events = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.events_raw")

    silver_events = (
        bronze_events
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
            # Extract first classification for FK
            col("classifications")[0]["segment"]["id"].alias("segment_id"),
            col("classifications")[0]["genre"]["id"].alias("genre_id"),
            col("_ingestion_timestamp")
        )
        .dropDuplicates(["event_id"])
        .filter(col("event_id").isNotNull())
    )

    silver_table = f"{CATALOG}.{SILVER_SCHEMA}.events"

    (
        silver_events.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(silver_table)
    )

    # Add primary key constraint (if not exists)
    add_primary_key_if_not_exists(silver_table, "events_pk", ["event_id"])

    # Enable liquid clustering (Databricks Runtime 15.2+)
    # Clustering by event_date and status_code for efficient query patterns
    spark.sql(f"""
        ALTER TABLE {silver_table}
        CLUSTER BY (event_date, status_code)
    """)

    print(f"✓ Created {silver_table} with {silver_events.count():,} records")

# COMMAND ----------

create_silver_events()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Table Creation - Event-Venue Bridge Table

# COMMAND ----------

def create_silver_event_venues():
    """
    Create many-to-many relationship between events and venues
    """

    bronze_events = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.events_raw")

    event_venues = (
        bronze_events
        .select(
            col("id").alias("event_id"),
            explode_outer(col("_embedded.venues")).alias("venue")
        )
        .select(
            "event_id",
            col("venue.id").alias("venue_id"),
            col("venue.name").alias("venue_name_snapshot")
        )
        .dropDuplicates(["event_id", "venue_id"])
        .filter(col("event_id").isNotNull() & col("venue_id").isNotNull())
    )

    silver_table = f"{CATALOG}.{SILVER_SCHEMA}.event_venues"

    (
        event_venues.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(silver_table)
    )

    # Add primary key constraint (if not exists)
    add_primary_key_if_not_exists(silver_table, "event_venues_pk", ["event_id", "venue_id"])

    # Add foreign key constraints (if not exist)
    add_foreign_key_if_not_exists(
        table_name=silver_table,
        constraint_name="event_venues_event_fk",
        fk_columns=["event_id"],
        reference_table=f"{CATALOG}.{SILVER_SCHEMA}.events",
        reference_columns=["event_id"]
    )
    
    add_foreign_key_if_not_exists(
        table_name=silver_table,
        constraint_name="event_venues_venue_fk",
        fk_columns=["venue_id"],
        reference_table=f"{CATALOG}.{SILVER_SCHEMA}.venues",
        reference_columns=["venue_id"]
    )

    print(f"✓ Created {silver_table} with {event_venues.count():,} records")

# COMMAND ----------

create_silver_event_venues()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Table Creation - Event-Attraction Bridge Table

# COMMAND ----------

def create_silver_event_attractions():
    """
    Create many-to-many relationship between events and attractions
    """

    bronze_events = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.events_raw")

    event_attractions = (
        bronze_events
        .select(
            col("id").alias("event_id"),
            explode_outer(col("_embedded.attractions")).alias("attraction")
        )
        .select(
            "event_id",
            col("attraction.id").alias("attraction_id"),
            col("attraction.name").alias("attraction_name_snapshot")
        )
        .dropDuplicates(["event_id", "attraction_id"])
        .filter(col("event_id").isNotNull() & col("attraction_id").isNotNull())
    )

    silver_table = f"{CATALOG}.{SILVER_SCHEMA}.event_attractions"

    (
        event_attractions.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(silver_table)
    )

    # Add primary key constraint (if not exists)
    add_primary_key_if_not_exists(silver_table, "event_attractions_pk", ["event_id", "attraction_id"])

    # Add foreign key constraints (if not exist)
    add_foreign_key_if_not_exists(
        table_name=silver_table,
        constraint_name="event_attractions_event_fk",
        fk_columns=["event_id"],
        reference_table=f"{CATALOG}.{SILVER_SCHEMA}.events",
        reference_columns=["event_id"]
    )
    
    add_foreign_key_if_not_exists(
        table_name=silver_table,
        constraint_name="event_attractions_attraction_fk",
        fk_columns=["attraction_id"],
        reference_table=f"{CATALOG}.{SILVER_SCHEMA}.attractions",
        reference_columns=["attraction_id"]
    )

    print(f"✓ Created {silver_table} with {event_attractions.count():,} records")

# COMMAND ----------

create_silver_event_attractions()

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
    "markets", "event_venues", "event_attractions"
]

for table in silver_tables:
    count = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.{table}").count()
    print(f"ticket_master.silver.{table}: {count:,} records")

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