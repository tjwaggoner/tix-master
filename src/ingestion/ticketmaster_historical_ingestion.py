# Databricks notebook source
"""
Ticketmaster API Historical Ingestion

ONE-TIME MANUAL RUN - Not part of scheduled job.

First-time full load of data from Ticketmaster API.
Fetches all upcoming events and reference data (venues, attractions, classifications).

Run this manually ONCE before starting the scheduled incremental job.
"""

# COMMAND ----------

# MAGIC %md
# MAGIC # Ticketmaster API Historical Ingestion
# MAGIC 
# MAGIC **⚠️ ONE-TIME MANUAL RUN - Not part of scheduled job**
# MAGIC 
# MAGIC This notebook performs the **historical full load** of data:
# MAGIC 1. Fetches all upcoming events (next 365 days)
# MAGIC 2. Fetches all venues, attractions, and classifications
# MAGIC 3. Writes raw JSON files to Unity Catalog Volumes
# MAGIC 4. Prepares data for Bronze layer processing

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

# API Request Configuration - Respects Ticketmaster API limits
PAGE_SIZE = 200
MAX_PAGES = 4  # Ticketmaster limit: (page * size) < 1000, so 200*5=1000 is max
               # Using 4 pages (0-3) = 800 records per date chunk to stay safe

# Date range for initial load - go back as far as possible, prioritize recent
# Ticketmaster typically keeps ~2 years of historical data
START_DATE_BASE = datetime.utcnow() - timedelta(days=730)  # 2 years back
END_DATE_BASE = datetime.utcnow() + timedelta(days=365)    # 1 year forward

print(f"Volume Path: {VOLUME_PATH}")
print(f"Event Date Range: {START_DATE_BASE.strftime('%Y-%m-%d')} to {END_DATE_BASE.strftime('%Y-%m-%d')}")
print(f"  (2 years historical + 1 year future)")
print(f"Strategy: Chunked by month to avoid API pagination limits (max 800 events per chunk)")
print(f"Note: Ticketmaster API limit is (page × size) < 1,000")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Unity Catalog Resources

# COMMAND ----------

# Create catalog, schema, and volume if they don't exist
print("Setting up Unity Catalog resources...")

# Set catalog context
spark.sql(f"USE CATALOG {CATALOG}")
print(f"✓ Using catalog: {CATALOG}")

# Create schema if it doesn't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")
print(f"✓ Schema ready: {CATALOG}.{BRONZE_SCHEMA}")

# Create volume if it doesn't exist
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}.{VOLUME_NAME}")
print(f"✓ Volume ready: {VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get API Key from Secrets

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

def generate_monthly_chunks(start_date: datetime, end_date: datetime):
    """
    Generate monthly date ranges to stay under API pagination limits.
    
    Returns list of (start, end) datetime tuples for each month.
    """
    chunks = []
    current = start_date
    
    while current < end_date:
        # Get the last day of current month
        if current.month == 12:
            chunk_end = current.replace(year=current.year + 1, month=1, day=1) - timedelta(seconds=1)
        else:
            chunk_end = current.replace(month=current.month + 1, day=1) - timedelta(seconds=1)
        
        # Don't go past the end date
        if chunk_end > end_date:
            chunk_end = end_date
        
        chunks.append((current, chunk_end))
        
        # Move to next month
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    
    return chunks

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
    filename = f"{entity_type}_{timestamp_str}_historical.json"
    output_path = f"{output_dir}/{filename}"
    
    # Write data
    json_str = json.dumps(data, indent=2)
    size_bytes = len(json_str.encode('utf-8'))
    size_mb = size_bytes / (1024 * 1024)
    
    dbutils.fs.put(output_path, json_str, overwrite=True)
    
    return output_path, size_mb

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Initial Load

# COMMAND ----------

if not API_KEY:
    raise ValueError("API key not available. Please configure Databricks secrets.")

# Initialize client
client = TicketmasterAPIClient(api_key=API_KEY, base_url=BASE_URL)

# Track timing
start_time = time.time()
start_timestamp = datetime.utcnow()

print("=" * 60)
print("STARTING HISTORICAL INGESTION")
print("=" * 60)
print(f"Start time: {start_timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}")
print()

# Process each endpoint
results = {}

for entity_type, endpoint in ENDPOINTS.items():
    print(f"📥 Fetching {entity_type} (HISTORICAL INGESTION)...")
    
    try:
        data = []
        
        if entity_type == 'events':
            # For events: fetch in monthly chunks to avoid pagination limits
            chunks = generate_monthly_chunks(START_DATE_BASE, END_DATE_BASE)
            print(f"  Fetching {len(chunks)} monthly chunks...")
            
            for i, (chunk_start, chunk_end) in enumerate(chunks, 1):
                start_str = chunk_start.strftime('%Y-%m-%dT%H:%M:%SZ')
                end_str = chunk_end.strftime('%Y-%m-%dT%H:%M:%SZ')
                
                print(f"  Chunk {i}/{len(chunks)}: {chunk_start.strftime('%Y-%m')} "
                      f"({chunk_start.strftime('%b %d')} to {chunk_end.strftime('%b %d, %Y')})")
                
                filters = {
                    'startDateTime': start_str,
                    'endDateTime': end_str,
                    'sort': 'date,desc'
                }
                
                chunk_data = client.fetch_paginated_data(
                    endpoint=endpoint,
                    page_size=PAGE_SIZE,
                    max_pages=MAX_PAGES,
                    additional_params=filters
                )
                
                data.extend(chunk_data)
                print(f"    → {len(chunk_data):,} events in this chunk (Total so far: {len(data):,})")
        else:
            # For venues, attractions, classifications: fetch without date filters
            # These datasets are smaller and don't hit pagination limits
            print(f"  Fetching all {entity_type}...")
            data = client.fetch_paginated_data(
                endpoint=endpoint,
                page_size=PAGE_SIZE,
                max_pages=MAX_PAGES,
                additional_params={}
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
print("HISTORICAL INGESTION SUMMARY")
print("=" * 60)

# Calculate timing
end_time = time.time()
end_timestamp = datetime.utcnow()
duration_seconds = end_time - start_time
duration_minutes = duration_seconds / 60
duration_hours = duration_minutes / 60

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
print()
print(f"Start time: {start_timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}")
print(f"End time:   {end_timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}")
if duration_hours >= 1:
    print(f"Duration:   {duration_hours:.2f} hours ({duration_minutes:.1f} minutes)")
elif duration_minutes >= 1:
    print(f"Duration:   {duration_minutes:.1f} minutes ({duration_seconds:.0f} seconds)")
else:
    print(f"Duration:   {duration_seconds:.1f} seconds")
print(f"Throughput: {total_records / duration_seconds:.0f} records/second")
print("=" * 60)

# COMMAND ----------

print("\n✓ Historical ingestion complete! Data is ready for Bronze layer processing.")

