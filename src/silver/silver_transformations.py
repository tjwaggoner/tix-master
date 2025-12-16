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
CATALOG = "ticketmaster"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

# Create Silver schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}")

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

    # Add primary key constraint
    spark.sql(f"""
        ALTER TABLE {silver_table}
        ADD CONSTRAINT venues_pk PRIMARY KEY (venue_id)
    """)

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

    spark.sql(f"""
        ALTER TABLE {silver_table}
        ADD CONSTRAINT attractions_pk PRIMARY KEY (attraction_id)
    """)

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

    silver_classifications = (
        bronze_classifications
        .select(
            col("segment.id").alias("segment_id"),
            col("segment.name").alias("segment_name"),
            col("genre.id").alias("genre_id"),
            col("genre.name").alias("genre_name"),
            col("subGenre.id").alias("subgenre_id"),
            col("subGenre.name").alias("subgenre_name"),
            col("type.id").alias("type_id"),
            col("type.name").alias("type_name"),
            col("subType.id").alias("subtype_id"),
            col("subType.name").alias("subtype_name"),
            col("_ingestion_timestamp")
        )
        # Create a composite key since classifications are hierarchical
        .withColumn("classification_id",
                    sha2(concat_ws("_",
                                   coalesce(col("segment_id"), lit("")),
                                   coalesce(col("genre_id"), lit("")),
                                   coalesce(col("subgenre_id"), lit(""))), 256))
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

    spark.sql(f"""
        ALTER TABLE {silver_table}
        ADD CONSTRAINT classifications_pk PRIMARY KEY (classification_id)
    """)

    print(f"✓ Created {silver_table} with {silver_classifications.count():,} records")

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

    spark.sql(f"""
        ALTER TABLE {silver_table}
        ADD CONSTRAINT markets_pk PRIMARY KEY (market_id)
    """)

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
        .partitionBy("event_date")
        .saveAsTable(silver_table)
    )

    # Add primary key
    spark.sql(f"""
        ALTER TABLE {silver_table}
        ADD CONSTRAINT events_pk PRIMARY KEY (event_id)
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

    # Add composite primary key
    spark.sql(f"""
        ALTER TABLE {silver_table}
        ADD CONSTRAINT event_venues_pk PRIMARY KEY (event_id, venue_id)
    """)

    # Add foreign key constraints
    spark.sql(f"""
        ALTER TABLE {silver_table}
        ADD CONSTRAINT event_venues_event_fk
        FOREIGN KEY (event_id) REFERENCES {CATALOG}.{SILVER_SCHEMA}.events(event_id)
    """)

    spark.sql(f"""
        ALTER TABLE {silver_table}
        ADD CONSTRAINT event_venues_venue_fk
        FOREIGN KEY (venue_id) REFERENCES {CATALOG}.{SILVER_SCHEMA}.venues(venue_id)
    """)

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

    # Add composite primary key
    spark.sql(f"""
        ALTER TABLE {silver_table}
        ADD CONSTRAINT event_attractions_pk PRIMARY KEY (event_id, attraction_id)
    """)

    # Add foreign key constraints
    spark.sql(f"""
        ALTER TABLE {silver_table}
        ADD CONSTRAINT event_attractions_event_fk
        FOREIGN KEY (event_id) REFERENCES {CATALOG}.{SILVER_SCHEMA}.events(event_id)
    """)

    spark.sql(f"""
        ALTER TABLE {silver_table}
        ADD CONSTRAINT event_attractions_attraction_fk
        FOREIGN KEY (attraction_id) REFERENCES {CATALOG}.{SILVER_SCHEMA}.attractions(attraction_id)
    """)

    print(f"✓ Created {silver_table} with {event_attractions.count():,} records")

# COMMAND ----------

create_silver_event_attractions()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Silver Layer

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show all silver tables
# MAGIC SHOW TABLES IN ticketmaster.silver;

# COMMAND ----------

# Display record counts
silver_tables = [
    "events", "venues", "attractions", "classifications",
    "markets", "event_venues", "event_attractions"
]

for table in silver_tables:
    count = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.{table}").count()
    print(f"ticketmaster.silver.{table}: {count:,} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View ERD in Unity Catalog
# MAGIC
# MAGIC Navigate to Unity Catalog UI:
# MAGIC 1. Open Catalog Explorer
# MAGIC 2. Select ticketmaster catalog
# MAGIC 3. Select silver schema
# MAGIC 4. Click "Lineage" tab to see ERD visualization
# MAGIC
# MAGIC The PK/FK constraints enable Unity Catalog to automatically generate the ERD!

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View constraints for a table
# MAGIC SHOW TBLPROPERTIES ticketmaster.silver.events;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example query using FK relationships
# MAGIC SELECT
# MAGIC   e.event_id,
# MAGIC   e.event_name,
# MAGIC   e.event_date,
# MAGIC   v.venue_name,
# MAGIC   v.city,
# MAGIC   v.state,
# MAGIC   a.attraction_name
# MAGIC FROM ticketmaster.silver.events e
# MAGIC INNER JOIN ticketmaster.silver.event_venues ev ON e.event_id = ev.event_id
# MAGIC INNER JOIN ticketmaster.silver.venues v ON ev.venue_id = v.venue_id
# MAGIC LEFT JOIN ticketmaster.silver.event_attractions ea ON e.event_id = ea.event_id
# MAGIC LEFT JOIN ticketmaster.silver.attractions a ON ea.attraction_id = a.attraction_id
# MAGIC WHERE e.event_date >= CURRENT_DATE()
# MAGIC ORDER BY e.event_date
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check for orphaned records (should be 0)
# MAGIC SELECT COUNT(*) as orphaned_event_venues
# MAGIC FROM ticketmaster.silver.event_venues ev
# MAGIC LEFT JOIN ticketmaster.silver.events e ON ev.event_id = e.event_id
# MAGIC WHERE e.event_id IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check for NULL primary keys (should be 0)
# MAGIC SELECT
# MAGIC   'events' as table_name,
# MAGIC   COUNT(*) as null_pk_count
# MAGIC FROM ticketmaster.silver.events
# MAGIC WHERE event_id IS NULL
# MAGIC UNION ALL
# MAGIC SELECT 'venues', COUNT(*)
# MAGIC FROM ticketmaster.silver.venues
# MAGIC WHERE venue_id IS NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Maintenance Operations

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Optimize all silver tables
# MAGIC OPTIMIZE ticketmaster.silver.events ZORDER BY (event_date);
# MAGIC OPTIMIZE ticketmaster.silver.venues;
# MAGIC OPTIMIZE ticketmaster.silver.attractions;
# MAGIC OPTIMIZE ticketmaster.silver.classifications;
# MAGIC OPTIMIZE ticketmaster.silver.markets;
# MAGIC OPTIMIZE ticketmaster.silver.event_venues;
# MAGIC OPTIMIZE ticketmaster.silver.event_attractions;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Analyze tables for query optimization
# MAGIC ANALYZE TABLE ticketmaster.silver.events COMPUTE STATISTICS FOR ALL COLUMNS;
# MAGIC ANALYZE TABLE ticketmaster.silver.venues COMPUTE STATISTICS FOR ALL COLUMNS;
