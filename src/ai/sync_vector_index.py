# Databricks notebook source
"""
Sync Vector Search Index with New Events
This refreshes the vector embeddings when new events are added
"""

# COMMAND ----------

# MAGIC %pip install databricks-vectorsearch --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient

# COMMAND ----------

# Configuration
CATALOG = "ticket_master"
SCHEMA = "gold"
VECTOR_SEARCH_ENDPOINT = "ticket_master_vector_search"
INDEX_NAME = "events_index"

# COMMAND ----------

# Initialize Vector Search client and sync index
vsc = VectorSearchClient(disable_notice=True)

index_full_name = f"{CATALOG}.{SCHEMA}.{INDEX_NAME}"

try:
    # Trigger index sync
    index = vsc.get_index(
        endpoint_name=VECTOR_SEARCH_ENDPOINT,
        index_name=index_full_name
    )
    
    # Sync the index
    index.sync()
    
    print(f"✓ Successfully synced vector index: {index_full_name}")
    print(f"  New events will be embedded and searchable")
    
except Exception as e:
    print(f"⚠️  Error syncing vector index: {e}")
    raise

