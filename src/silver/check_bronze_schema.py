# Databricks notebook source
"""
Diagnostic notebook to check Bronze table schemas
"""

# COMMAND ----------

CATALOG = "ticket_master"
BRONZE_SCHEMA = "bronze"

# Set catalog context
spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Classifications Raw Table

# COMMAND ----------

table_name = f"{CATALOG}.{BRONZE_SCHEMA}.classifications_raw"

print(f"Checking schema for: {table_name}\n")
print("=" * 80)

# Check if table exists
if spark.catalog.tableExists(table_name):
    df = spark.table(table_name)
    
    print(f"Row count: {df.count():,}\n")
    
    print("SCHEMA:")
    print("=" * 80)
    df.printSchema()
    
    print("\n" + "=" * 80)
    print("SAMPLE DATA (first 3 rows):")
    print("=" * 80)
    df.show(3, truncate=False, vertical=True)
    
    print("\n" + "=" * 80)
    print("COLUMN NAMES:")
    print("=" * 80)
    for col_name in df.columns:
        print(f"  - {col_name}")
else:
    print(f"❌ Table does not exist: {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Events Raw (for nested classifications structure)

# COMMAND ----------

events_table = f"{CATALOG}.{BRONZE_SCHEMA}.events_raw"

if spark.catalog.tableExists(events_table):
    events_df = spark.table(events_table)
    
    print(f"\n{'=' * 80}")
    print("EVENTS TABLE - Classifications Column Structure:")
    print("=" * 80)
    
    # Check if classifications column exists
    if "classifications" in events_df.columns:
        events_df.select("classifications").printSchema()
        
        print("\nSample classifications data:")
        events_df.select("id", "name", "classifications").show(3, truncate=False, vertical=True)
    else:
        print("No 'classifications' column found in events_raw")
else:
    print(f"❌ Events table does not exist: {events_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Venues Raw

# COMMAND ----------

venues_table = f"{CATALOG}.{BRONZE_SCHEMA}.venues_raw"

if spark.catalog.tableExists(venues_table):
    venues_df = spark.table(venues_table)
    
    print(f"\n{'=' * 80}")
    print("VENUES TABLE SCHEMA:")
    print("=" * 80)
    print(f"Row count: {venues_df.count():,}\n")
    venues_df.printSchema()
else:
    print(f"❌ Venues table does not exist: {venues_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Attractions Raw

# COMMAND ----------

attractions_table = f"{CATALOG}.{BRONZE_SCHEMA}.attractions_raw"

if spark.catalog.tableExists(attractions_table):
    attractions_df = spark.table(attractions_table)
    
    print(f"\n{'=' * 80}")
    print("ATTRACTIONS TABLE SCHEMA:")
    print("=" * 80)
    print(f"Row count: {attractions_df.count():,}\n")
    attractions_df.printSchema()
else:
    print(f"❌ Attractions table does not exist: {attractions_table}")

