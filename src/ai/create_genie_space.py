# file: src/create_genie_space.py
import os
import json
import sys
import requests
import yaml
from pathlib import Path

# Load warehouse_id from databricks.yml
def load_warehouse_id():
    config_path = Path(__file__).parent / "databricks.yml"
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    return config.get('variables', {}).get('warehouse_id', {}).get('default')

HOST = os.environ.get("DATABRICKS_HOST")  # e.g., https://<your-workspace>
TOKEN = os.environ.get("DATABRICKS_TOKEN")  # OAuth PAT or U2M token
WAREHOUSE_ID = load_warehouse_id()  # Load from databricks.yml
SPACE_TITLE = os.environ.get("GENIE_SPACE_TITLE", "Ticketmaster Analytics Space")

if not HOST or not TOKEN or not WAREHOUSE_ID:
    sys.stderr.write("Missing DATABRICKS_HOST, DATABRICKS_TOKEN, or warehouse_id in databricks.yml.\n")
    sys.exit(2)

session = requests.Session()
session.headers.update({"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"})

def list_spaces():
    resp = session.get(f"{HOST}/api/2.0/genie/spaces")
    resp.raise_for_status()
    return resp.json().get("spaces", [])

def create_space():
    # Ticketmaster analytics serialized_space configuration
    serialized_space = {
        "version": 1,
        "config": {
            "sample_questions": [
                {"id": "q1", "question": ["How many events are scheduled for next month?"]},
                {"id": "q2", "question": ["What are the top 10 venues by event count?"]},
                {"id": "q3", "question": ["Show me events by attraction type and genre"]},
                {"id": "q4", "question": ["What is the average ticket price by city?"]},
                {"id": "q5", "question": ["How many weekend vs weekday events are there?"]},
            ]
        },
        "data_sources": {
            "tables": [
                {"identifier": "ticket_master.gold.fact_events"},
                {"identifier": "ticket_master.gold.dim_venue"},
                {"identifier": "ticket_master.gold.dim_attraction"},
                {"identifier": "ticket_master.gold.dim_date"},
                {"identifier": "ticket_master.gold.dim_classification"},
                {"identifier": "ticket_master.gold.dim_market"},
                {"identifier": "ticket_master.gold.mv_events_by_date_venue"},
                {"identifier": "ticket_master.gold.mv_events_by_attraction"},
                {"identifier": "ticket_master.gold.mv_monthly_summary"},
            ]
        },
        "instructions": {
            "text_instructions": [
                {"id": "i1", "content": ["Use fact_events as the main fact table. Join to dimensions using surrogate keys (venue_sk, attraction_sk, event_date_key)."]},
                {"id": "i2", "content": ["Price range is defined by price_min and price_max columns in fact_events."]},
                {"id": "i3", "content": ["For date-based queries, join fact_events to dim_date using event_date_key. Use dim_date for year, month, quarter, and weekend analysis."]},
                {"id": "i4", "content": ["Venue information (city, state, country) is in dim_venue. Use venue_sk to join."]},
                {"id": "i5", "content": ["Attraction details (name, type, segment, genre) are in dim_attraction. Use attraction_sk to join."]},
                {"id": "i6", "content": ["Pre-aggregated views (mv_events_by_date_venue, mv_events_by_attraction, mv_monthly_summary) can be used for faster queries."]},
            ]
        }
    }

    payload = {
        "title": SPACE_TITLE,
        "description": "Space for analyzing Ticketmaster events, venues, attractions, and pricing across the gold star schema",
        "warehouse_id": WAREHOUSE_ID,
        # API expects serialized_space as an escaped JSON string
        "serialized_space": json.dumps(serialized_space)
    }
    resp = session.post(f"{HOST}/api/2.0/genie/spaces", data=json.dumps(payload))
    resp.raise_for_status()
    return resp.json()

def main():
    # Try to find by title; if exists, reuse
    existing = [s for s in list_spaces() if s.get("title") == SPACE_TITLE]
    if existing:
        space = existing[0]
        print(space["space_id"])  # stdout -> capture for bundles variable
        return

    space = create_space()
    print(space["space_id"])

if __name__ == "__main__":
    main()
