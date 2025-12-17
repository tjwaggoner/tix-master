# Databricks notebook source
"""
Ticketmaster API Ingestion

Fetches data from Ticketmaster API and lands raw JSON files to Unity Catalog Volumes
for processing in the Bronze layer.
"""

# COMMAND ----------

# MAGIC %md
# MAGIC # Ticketmaster API Ingestion
# MAGIC 
# MAGIC This notebook:
# MAGIC 1. Fetches data from Ticketmaster Discovery API
# MAGIC 2. Writes raw JSON files to Unity Catalog Volumes
# MAGIC 3. Prepares data for Bronze layer processing

# COMMAND ----------

import json
from datetime import datetime
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
MAX_PAGES = 10  # Limit for testing, increase for production

# Date range for events - fetch upcoming events
from datetime import datetime, timedelta
START_DATE = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')  # Now
END_DATE = (datetime.utcnow() + timedelta(days=365)).strftime('%Y-%m-%dT%H:%M:%SZ')  # Next year

print(f"Volume Path: {VOLUME_PATH}")
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

def write_to_volume(data: List[Dict], entity_type: str, volume_path: str) -> str:
    """
    Write data as JSON to Unity Catalog Volume
    
    Returns:
        Path to the written file
    """
    # Create directory structure: entity_type/year/month/day/
    now = datetime.utcnow()
    date_path = now.strftime("%Y/%m/%d")
    
    output_dir = f"{volume_path}/{entity_type}/{date_path}"
    dbutils.fs.mkdirs(output_dir)
    
    # Create filename with timestamp
    timestamp_str = now.strftime("%Y%m%d_%H%M%S")
    filename = f"{entity_type}_{timestamp_str}.json"
    output_path = f"{output_dir}/{filename}"
    
    # Write data
    json_str = json.dumps(data, indent=2)
    dbutils.fs.put(output_path, json_str, overwrite=True)
    
    return output_path

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
            output_path = write_to_volume(
                data=data,
                entity_type=entity_type,
                volume_path=VOLUME_PATH
            )
            results[entity_type] = {
                'count': len(data),
                'path': output_path,
                'status': 'success'
            }
            print(f"  ✓ Wrote {len(data)} items to {output_path}\n")
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
print("INGESTION SUMMARY")
print("=" * 60)

total_records = 0
for entity_type, result in results.items():
    status = result.get('status')
    count = result.get('count', 0)
    
    if status == 'success':
        print(f"✓ {entity_type:20s}: {count:,} records")
        total_records += count
    elif status == 'no_data':
        print(f"⚠ {entity_type:20s}: No data")
    else:
        print(f"✗ {entity_type:20s}: Error - {result.get('error')}")

print("=" * 60)
print(f"Total records ingested: {total_records:,}")
print(f"Volume path: {VOLUME_PATH}")
print("=" * 60)

# COMMAND ----------

print("\n✓ Ingestion complete! Data is ready for Bronze layer processing.")

