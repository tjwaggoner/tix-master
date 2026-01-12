# Ticketmaster Lakeview Dashboard - Upcoming Events Analytics

This guide explains how to create a Lakeview dashboard for analyzing upcoming Ticketmaster events using the **Databricks SDK API**.

## Overview

The dashboard provides comprehensive analytics for events over the **next 365 days**, including:

### **Key Performance Indicators (KPIs)**
- Total upcoming events
- Unique venues hosting events
- Unique attractions/performers
- Average ticket price

### **Time-Based Analysis**
- **Monthly Timeline**: Event trends over next 12 months
- **Weekly Timeline**: Short-term event trends (next 90 days)
- **Segment Timeline**: Event distribution by classification over time

### **Geographic Analysis**
- **Interactive Map**: Event locations plotted by venue latitude/longitude with event counts
- **Events by State**: Top 20 states by event count
- **Events by City**: Top 20 cities by event count

### **Classification & Genre**
- Events segmented by classification (Music, Sports, Arts, Theater, etc.)
- Top 20 genres by event count
- Price analysis by segment

### **Venue & Attraction Analysis**
- Top 20 venues by event count
- Top 20 attractions/performers by event count
- Unique venue counts per attraction

### **Price Analysis**
- Price distribution histogram
- Average/min/max prices by segment

---

## Prerequisites

### 1. Databricks Workspace
- Unity Catalog enabled
- Gold layer tables populated with data

### 2. SQL Warehouse
Get your SQL Warehouse ID:

```bash
# List warehouses
databricks sql warehouses list

# Or from UI: SQL Warehouses → Select warehouse → Copy ID from URL
```

### 3. Python Environment

```bash
# Install Databricks SDK
pip install databricks-sdk

# Or if using requirements.txt
pip install -r requirements.txt
```

### 4. Authentication

Set up Databricks authentication:

**Option A: Environment Variables**
```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-token-here"
```

**Option B: OAuth (Recommended)**
```bash
databricks auth login --host https://your-workspace.cloud.databricks.com
```

---

## Creating the Dashboard Using Databricks SDK API

### Quick Start

```bash
# Set warehouse ID (find using: databricks sql warehouses list)
export DATABRICKS_WAREHOUSE_ID="your_warehouse_id"

# Run the script
python src/ai/lakeview/create_dashboard.py
```

### Full Usage

```bash
# Basic usage with warehouse ID
python src/ai/lakeview/create_dashboard.py <warehouse_id>

# Specify custom parent folder
python src/ai/lakeview/create_dashboard.py <warehouse_id> "/Users/your.email@company.com/dashboards"

# Using environment variable
export DATABRICKS_WAREHOUSE_ID="f4040a30fe978741"
python src/ai/lakeview/create_dashboard.py
```

### Example Output

```
Creating dashboard: Ticketmaster Upcoming Events - Next 365 Days
Using warehouse: f4040a30fe978741

Dashboard Features:
  • Time Range: Next 365 days from today
  • KPIs: Total events, venues, attractions, avg price
  • Geographic Map: Event locations with counts
  • Segment Analysis: Events by classification
  • Genre Breakdown: Top genres by event count
  • Timeline: Monthly and weekly trends
  • Location Analysis: Events by state and city
  • Price Analysis: Distribution and segment averages
  • Top Performers: Venues and attractions

✅ Dashboard created successfully!
Dashboard ID: 01j2k3l4m5n6o7p8q9
Dashboard Path: /Users/your.email@company.com/Ticketmaster Upcoming Events - Next 365 Days

View your dashboard at:
https://your-workspace.cloud.databricks.com/sql/dashboards/01j2k3l4m5n6o7p8q9
```

---

## Dashboard Architecture

### Data Source
All queries use the Gold layer star schema:
- `ticket_master.gold.fact_events` - Event fact table
- `ticket_master.gold.dim_venue` - Venue dimension (SCD Type 2)
- `ticket_master.gold.dim_attraction` - Attraction dimension (SCD Type 2)
- `ticket_master.gold.dim_classification` - Classification dimension (SCD Type 2)
- `ticket_master.gold.dim_date` - Date dimension
- `ticket_master.gold.bridge_event_attractions` - Event-attraction bridge table

### Query Filters
All queries filter for:
- **Time Range**: `event_date >= CURRENT_DATE() AND event_date <= CURRENT_DATE() + INTERVAL 365 DAYS`
- **Non-test Events**: `is_test = FALSE`
- **Current Dimensions**: `is_current = TRUE` (for SCD Type 2 joins)

### Dashboard Layout

```
┌─────────────────────────────────────────────────────────────┐
│  Row 1: KPIs (4 cards)                                      │
│  [Total Events] [Venues] [Attractions] [Avg Price]          │
├─────────────────────────────────────────────────────────────┤
│  Row 2: Monthly Timeline (line chart)                       │
│  Event counts and prices over next 12 months                │
├─────────────────────────────────────────────────────────────┤
│  Row 3: Geographic Map (full width)                         │
│  Interactive map with venue locations and event counts      │
├─────────────────────────────────────────────────────────────┤
│  Row 4: Segment Analysis                                    │
│  [Events by Segment] [Price by Segment]                     │
├─────────────────────────────────────────────────────────────┤
│  Row 5: Genre & Timeline                                    │
│  [Top Genres] [Segment Timeline]                            │
├─────────────────────────────────────────────────────────────┤
│  Row 6: Geographic Breakdown                                │
│  [Events by State] [Events by City]                         │
├─────────────────────────────────────────────────────────────┤
│  Row 7: Top Performers                                      │
│  [Top Venues] [Top Attractions]                             │
├─────────────────────────────────────────────────────────────┤
│  Row 8: Weekly Trends & Prices                              │
│  [Weekly Timeline] [Price Distribution]                     │
└─────────────────────────────────────────────────────────────┘
```

---

## Customization

### Modify Time Range

Edit queries in `create_dashboard.py`:

```python
# Change from 365 days to 180 days (6 months)
WHERE event_date >= CURRENT_DATE()
  AND event_date <= CURRENT_DATE() + INTERVAL 180 DAYS

# Or focus on next 30 days only
WHERE event_date >= CURRENT_DATE()
  AND event_date <= CURRENT_DATE() + INTERVAL 30 DAYS
```

### Add Filters by Segment

```python
"filtered_events": """
    SELECT
      f.event_name,
      f.event_date,
      c.segment_name
    FROM ticket_master.gold.fact_events f
    INNER JOIN ticket_master.gold.dim_classification c
      ON f.classification_sk_fk = c.classification_sk AND c.is_current = TRUE
    WHERE f.is_test = FALSE
      AND f.event_date >= CURRENT_DATE()
      AND f.event_date <= CURRENT_DATE() + INTERVAL 365 DAYS
      AND c.segment_name IN ('Music', 'Sports')  -- Filter specific segments
"""
```

### Add Geographic Filters

```python
# Filter by specific states
WHERE v.state IN ('CA', 'NY', 'TX', 'FL')

# Filter by specific cities
WHERE v.city IN ('Los Angeles', 'New York', 'Chicago', 'Miami')

# Filter by region (example: West Coast)
WHERE v.state IN ('CA', 'OR', 'WA')
```

### Add New Visualizations

1. Add new query to `QUERIES` dict:

```python
QUERIES = {
    # ... existing queries ...

    "my_custom_query": """
        SELECT
          f.event_date,
          COUNT(*) as event_count
        FROM ticket_master.gold.fact_events f
        WHERE f.is_test = FALSE
          AND f.event_date >= CURRENT_DATE()
        GROUP BY f.event_date
    """
}
```

2. Add widget to layout:

```python
{
    "widget": {"name": "my_custom_query"},
    "position": {"x": 0, "y": 48, "width": 12, "height": 6}
}
```

3. Re-run the script to create updated dashboard

---

## Dashboard Queries Reference

### Geographic Map Query

The geographic map plots events by venue location:

```sql
SELECT
  v.venue_name,
  v.city,
  v.state,
  v.latitude,
  v.longitude,
  COUNT(DISTINCT f.event_id) as event_count,
  AVG(f.price_max) as avg_price,
  MIN(f.event_date) as earliest_event,
  MAX(f.event_date) as latest_event
FROM ticket_master.gold.fact_events f
INNER JOIN ticket_master.gold.dim_venue v
  ON f.venue_sk_fk = v.venue_sk AND v.is_current = TRUE
WHERE f.is_test = FALSE
  AND f.event_date >= CURRENT_DATE()
  AND f.event_date <= CURRENT_DATE() + INTERVAL 365 DAYS
  AND v.latitude IS NOT NULL
  AND v.longitude IS NOT NULL
GROUP BY v.venue_name, v.city, v.state, v.latitude, v.longitude
ORDER BY event_count DESC
```

**Key Features:**
- Uses `latitude` and `longitude` for map plotting
- Aggregates events per venue
- Shows earliest and latest event dates
- Includes average ticket price

---

## Troubleshooting

### "Warehouse not found"
**Problem**: Invalid warehouse ID

**Solution**:
```bash
# List available warehouses
databricks sql warehouses list

# Copy the ID from output
# Example: f4040a30fe978741
```

### "Table or view not found"
**Problem**: Gold layer tables don't exist

**Solution**:
1. Run historical ingestion: `src/ingestion/ticketmaster_historical_ingestion.py`
2. Verify tables exist:
```sql
SHOW TABLES IN ticket_master.gold;
```

### "Permission denied"
**Problem**: Missing permissions

**Solution**:
- Ensure you have `CREATE_DASHBOARD` permission
- Verify SQL Warehouse access permissions
- Check Unity Catalog permissions on gold schema

### "Dashboard not appearing in UI"
**Problem**: Dashboard created but not visible

**Solution**:
- Check parent folder path is valid
- Look in your user folder: `/Users/your.email@company.com/`
- Verify workspace permissions
- Refresh the Databricks UI

### Authentication Errors

**Problem**: `AuthenticationError` when running script

**Solution**:
```bash
# Re-authenticate
databricks auth login --host https://your-workspace.cloud.databricks.com

# Or set credentials
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="your-token"
```

---

## Dashboard Management

### Update Existing Dashboard

To update the dashboard, modify the script and run it again. The SDK will create a new dashboard (it doesn't update existing ones automatically).

To update in place:
1. Delete old dashboard via UI
2. Re-run script with same settings

Or programmatically:
```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Delete old dashboard
w.lakeview.delete(dashboard_id="old_dashboard_id")

# Create new one
create_lakeview_dashboard(warehouse_id="...")
```

### Schedule Dashboard Refresh

Dashboards automatically refresh when opened. To schedule automatic refreshes:
1. Open dashboard in UI
2. Click "Schedule" button
3. Set refresh frequency (hourly, daily, etc.)

### Share Dashboard

```python
# Add permissions via SDK
w.lakeview.update(
    dashboard_id="your_dashboard_id",
    dashboard=Dashboard(
        # ... existing config ...
    )
)
```

Or via UI:
1. Open dashboard
2. Click "Share" button
3. Add users/groups with appropriate permissions

---

## Performance Optimization

### Query Performance

All queries are optimized for the star schema:
- Use surrogate key joins (`venue_sk_fk = venue_sk`)
- Filter on `is_current = TRUE` for SCD Type 2 dimensions
- Filter on `event_date` for partition pruning
- Use `is_test = FALSE` to exclude test data

### Dashboard Load Time

For faster dashboard loading:
- Limit date ranges (use 90 days instead of 365 for detailed views)
- Use `LIMIT` clauses on large result sets
- Consider materializing complex queries as views

### Liquid Clustering

The fact_events table uses liquid clustering on `event_date` and `venue_sk_fk`:
```sql
CLUSTER BY (event_date, venue_sk_fk)
```

This optimizes query performance for date-based filtering.

---

## API Reference

### Databricks SDK Lakeview Methods

```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Create dashboard
dashboard = w.lakeview.create(
    display_name="Dashboard Name",
    warehouse_id="warehouse_id",
    serialized_dashboard=json.dumps({...}),
    parent_path="/Users/user@company.com/folder"
)

# List dashboards
dashboards = w.lakeview.list()

# Get dashboard
dashboard = w.lakeview.get(dashboard_id="dashboard_id")

# Delete dashboard
w.lakeview.delete(dashboard_id="dashboard_id")

# Publish dashboard
w.lakeview.publish(dashboard_id="dashboard_id")
```

### Dashboard Configuration Schema

```python
{
    "display_name": str,           # Dashboard title
    "warehouse_id": str,            # SQL Warehouse ID
    "parent_path": str,             # Optional folder path
    "serialized_dashboard": str     # JSON string with dashboard config
}
```

### Serialized Dashboard Structure

```python
{
    "pages": [
        {
            "name": str,            # Internal page name
            "displayName": str,     # Display name
            "layout": [
                {
                    "widget": {"name": str},
                    "position": {
                        "x": int,       # X coordinate (0-11)
                        "y": int,       # Y coordinate
                        "width": int,   # Width (1-12)
                        "height": int   # Height
                    }
                }
            ]
        }
    ],
    "datasets": [
        {
            "name": str,           # Query identifier
            "displayName": str,    # Display name
            "query": str          # SQL query
        }
    ]
}
```

---

## Next Steps

### 1. Enhance Visualizations
- Add drill-down capabilities
- Create dashboard filters for interactive analysis
- Add trend indicators (up/down arrows)

### 2. Scheduled Reports
- Set up email reports for stakeholders
- Schedule automatic refreshes
- Export to PDF/PowerPoint

### 3. AI/BI Integration
- Connect Genie space for natural language queries
- Enable AI-powered insights
- Add predictive analytics

### 4. Alerting
- Configure alerts for unusual patterns
- Monitor event volume anomalies
- Track price fluctuations

### 5. Additional Dashboards
- Create segment-specific dashboards (Music, Sports, etc.)
- Build venue operator dashboards
- Develop executive summary dashboard

---

## Resources

- **Databricks Lakeview Documentation**: https://docs.databricks.com/dashboards/
- **Databricks SDK for Python**: https://docs.databricks.com/dev-tools/sdk-python.html
- **SQL Warehouses**: https://docs.databricks.com/sql/admin/sql-warehouses.html
- **Unity Catalog**: https://docs.databricks.com/data-governance/unity-catalog/
- **Liquid Clustering**: https://docs.databricks.com/delta/clustering.html

---

**Last Updated**: January 12, 2026
