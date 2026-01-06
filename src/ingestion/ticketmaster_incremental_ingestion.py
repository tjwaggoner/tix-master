# Databricks notebook source
"""
Ticketmaster API Incremental Ingestion

Fetches NEW data from Ticketmaster API since the last job run.
Lands raw JSON files to Unity Catalog Volumes for processing in the Bronze layer.

Rate Limiting: Configured per Ticketmaster API constraints (5 req/sec, size×page<1000).
Note: Testing revealed undocumented max page size of ~200 (larger sizes return 0 events).
See API_INFO.md for details.
"""

# COMMAND ----------

# MAGIC %md
# MAGIC # Ticketmaster API Incremental Ingestion
# MAGIC 
# MAGIC This notebook performs **incremental data ingestion**:
# MAGIC 1. Determines the last ingestion timestamp from Bronze tables
# MAGIC 2. Fetches only NEW data since last run from Ticketmaster API
# MAGIC 3. Writes raw JSON files to Unity Catalog Volumes
# MAGIC 4. Prepares data for Bronze layer processing
# MAGIC 
# MAGIC **Note:** For first-time loads, use `ticketmaster_historical_ingestion.py` instead.

# COMMAND ----------

import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import requests

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Configuration
CATALOG = "ticket_master"
BRONZE_SCHEMA = "bronze"
VOLUME_NAME = "raw_data"
VOLUME_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{VOLUME_NAME}"

BASE_URL = "https://app.ticketmaster.com/discovery/v2"
ENDPOINTS = {
    'events': '/events.json',
    'venues': '/venues.json',
    'attractions': '/attractions.json',
    'classifications': '/classifications.json'
}

# API Request Configuration
PAGE_SIZE = 200
MAX_PAGES = 50  # Reasonable limit for incremental loads

# COMMAND ----------

# MAGIC %md
# MAGIC ## Determine Incremental Load Date Range

# COMMAND ----------

# Date range for events - fetch incrementally since last run
from datetime import datetime, timedelta

# Get the last ingestion timestamp from Bronze layer
try:
    # Check if bronze tables exist
    if not spark.catalog.tableExists(f"{CATALOG}.{BRONZE_SCHEMA}.events_raw"):
        error_msg = (
            "❌ ERROR: No Bronze tables found. "
            "Please run 'ticketmaster_historical_ingestion.py' first to perform historical data load."
        )
        print(error_msg)
        dbutils.notebook.exit(error_msg)
    
    # Get the most recent ingestion timestamp across all bronze tables
    last_ingestion = spark.sql(f"""
        SELECT MAX(_ingestion_timestamp) as last_run
        FROM (
            SELECT MAX(_ingestion_timestamp) as _ingestion_timestamp 
            FROM {CATALOG}.{BRONZE_SCHEMA}.events_raw
            UNION ALL
            SELECT MAX(_ingestion_timestamp) as _ingestion_timestamp 
            FROM {CATALOG}.{BRONZE_SCHEMA}.venues_raw
            WHERE EXISTS (SELECT 1 FROM {CATALOG}.{BRONZE_SCHEMA}.venues_raw LIMIT 1)
            UNION ALL
            SELECT MAX(_ingestion_timestamp) as _ingestion_timestamp 
            FROM {CATALOG}.{BRONZE_SCHEMA}.attractions_raw
            WHERE EXISTS (SELECT 1 FROM {CATALOG}.{BRONZE_SCHEMA}.attractions_raw LIMIT 1)
            UNION ALL
            SELECT MAX(_ingestion_timestamp) as _ingestion_timestamp 
            FROM {CATALOG}.{BRONZE_SCHEMA}.classifications_raw
            WHERE EXISTS (SELECT 1 FROM {CATALOG}.{BRONZE_SCHEMA}.classifications_raw LIMIT 1)
        )
    """).collect()[0]['last_run']
    
    if not last_ingestion:
        error_msg = (
            "❌ ERROR: Bronze tables exist but contain no data. "
            "Please run 'ticketmaster_historical_ingestion.py' first to perform historical data load."
        )
        print(error_msg)
        dbutils.notebook.exit(error_msg)
    
    # Add 1-day lookback buffer to catch any updates
    lookback = timedelta(days=1)
    start_time = last_ingestion - lookback
    START_DATE = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')
    
    print(f"✓ Incremental load starting from: {START_DATE}")
    print(f"  (Last ingestion: {last_ingestion.strftime('%Y-%m-%dT%H:%M:%SZ')} with 1-day lookback)")
        
except Exception as e:
    error_msg = f"❌ ERROR: Could not determine last run time: {e}"
    print(error_msg)
    dbutils.notebook.exit(error_msg)

# Always look ahead 365 days from now for upcoming events
END_DATE = (datetime.utcnow() + timedelta(days=365)).strftime('%Y-%m-%dT%H:%M:%SZ')

print(f"\nVolume Path: {VOLUME_PATH}")
print(f"Event Date Range: {START_DATE} to {END_DATE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get API Key from Secrets
# MAGIC 
# MAGIC Set up your secret with:
# MAGIC ```
# MAGIC databricks secrets create-scope --scope tix-master
# MAGIC databricks secrets put --scope tix-master --key ticketmaster-api-key
# MAGIC ```

# COMMAND ----------

# Get API key from Databricks secrets
try:
    API_KEY = dbutils.secrets.get(scope="tix-master", key="ticketmaster-api-key")
    print("✓ API key retrieved from secrets")
except Exception as e:
    print(f"⚠️  Warning: Could not retrieve API key from secrets: {e}")
    print("Please set up Databricks secrets or provide API key manually")
    API_KEY = None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ticketmaster API Client

# COMMAND ----------

class TicketmasterAPIClient:
    """Client for Ticketmaster Discovery API"""
    
    def __init__(self, api_key: str, base_url: str):
        self.api_key = api_key
        self.base_url = base_url
    
    def fetch_paginated_data(
        self,
        endpoint: str,
        page_size: int = 200,
        max_pages: Optional[int] = None,
        additional_params: Optional[Dict] = None
    ) -> List[Dict]:
        """Fetch all pages of data from an endpoint"""
        all_items = []
        page = 0
        
        while True:
            if max_pages and page >= max_pages:
                print(f"  Reached max pages limit: {max_pages}")
                break
            
            params = {
                'apikey': self.api_key,
                'size': page_size,
                'page': page
            }
            
            # Add additional filters (e.g., date range, sorting)
            if additional_params:
                params.update(additional_params)
            
            try:
                url = f"{self.base_url}{endpoint}"
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                # Extract items
                items_key = self._get_items_key(endpoint)
                embedded = data.get('_embedded', {})
                items = embedded.get(items_key, [])
                
                if not items:
                    print(f"  No more items on page {page}")
                    break
                
                all_items.extend(items)
                print(f"  Page {page}: {len(items)} items (Total: {len(all_items)})")
                
                # Check if there are more pages
                page_info = data.get('page', {})
                total_pages = page_info.get('totalPages', 1)
                
                if page >= total_pages - 1:
                    print(f"  Reached last page")
                    break
                
                page += 1
                
            except Exception as e:
                print(f"  Error on page {page}: {str(e)}")
                break
        
        return all_items
    
    def _get_items_key(self, endpoint: str) -> str:
        """Get the response key name based on endpoint"""
        if 'events' in endpoint:
            return 'events'
        elif 'venues' in endpoint:
            return 'venues'
        elif 'attractions' in endpoint:
            return 'attractions'
        elif 'classifications' in endpoint:
            return 'classifications'
        return 'items'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Volume Writer

# COMMAND ----------

def write_to_volume(data: List[Dict], entity_type: str, volume_path: str) -> tuple:
    """
    Write data as JSON to Unity Catalog Volume
    
    Returns:
        Tuple of (path, size_in_mb)
    """
    # Create directory structure: entity_type/year/month/day/
    now = datetime.utcnow()
    date_path = now.strftime("%Y/%m/%d")
    
    output_dir = f"{volume_path}/{entity_type}/{date_path}"
    dbutils.fs.mkdirs(output_dir)
    
    # Create filename with timestamp
    timestamp_str = now.strftime("%Y%m%d_%H%M%S")
    filename = f"{entity_type}_{timestamp_str}_incremental.json"
    output_path = f"{output_dir}/{filename}"
    
    # Write data
    json_str = json.dumps(data, indent=2)
    size_bytes = len(json_str.encode('utf-8'))
    size_mb = size_bytes / (1024 * 1024)
    
    dbutils.fs.put(output_path, json_str, overwrite=True)
    
    return output_path, size_mb

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Ingestion

# COMMAND ----------

if not API_KEY:
    raise ValueError("API key not available. Please configure Databricks secrets.")

# Initialize client
client = TicketmasterAPIClient(api_key=API_KEY, base_url=BASE_URL)

print("Starting Ticketmaster API ingestion...\n")

# Process each endpoint
results = {}

for entity_type, endpoint in ENDPOINTS.items():
    print(f"📥 Fetching {entity_type}...")
    
    try:
        # Set up filters based on entity type
        filters = {}
        
        if entity_type == 'events':
            # For events: get upcoming events, sorted by date
            filters = {
                'startDateTime': START_DATE,
                'endDateTime': END_DATE,
                'sort': 'date,asc'  # Sort by date ascending (soonest first)
            }
            print(f"  Filter: Upcoming events from {START_DATE[:10]} to {END_DATE[:10]}")
        
        # Fetch data
        data = client.fetch_paginated_data(
            endpoint=endpoint,
            page_size=PAGE_SIZE,
            max_pages=MAX_PAGES,
            additional_params=filters
        )
        
        if data:
            # Write to volume
            output_path, size_mb = write_to_volume(
                data=data,
                entity_type=entity_type,
                volume_path=VOLUME_PATH
            )
            results[entity_type] = {
                'count': len(data),
                'path': output_path,
                'size_mb': size_mb,
                'status': 'success'
            }
            print(f"  ✓ Wrote {len(data):,} items ({size_mb:.2f} MB) to {output_path}\n")
        else:
            results[entity_type] = {
                'count': 0,
                'status': 'no_data'
            }
            print(f"  ⚠️  No data found\n")
    
    except Exception as e:
        results[entity_type] = {
            'status': 'error',
            'error': str(e)
        }
        print(f"  ✗ Error: {str(e)}\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("=" * 60)
print("INCREMENTAL INGESTION SUMMARY")
print("=" * 60)

total_records = 0
total_mb = 0
for entity_type, result in results.items():
    status = result.get('status')
    count = result.get('count', 0)
    size_mb = result.get('size_mb', 0)
    
    if status == 'success':
        print(f"✓ {entity_type:20s}: {count:,} records ({size_mb:.2f} MB)")
        total_records += count
        total_mb += size_mb
    elif status == 'no_data':
        print(f"⚠ {entity_type:20s}: No data")
    else:
        print(f"✗ {entity_type:20s}: Error - {result.get('error')}")

print("=" * 60)
print(f"Total records ingested: {total_records:,}")
print(f"Total data size: {total_mb:.2f} MB")
print(f"Volume path: {VOLUME_PATH}")
print("=" * 60)

# COMMAND ----------

print("\n✓ Ingestion complete! Data is ready for Bronze layer processing.")

