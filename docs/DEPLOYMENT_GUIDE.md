# Ticketmaster Medallion Architecture - Deployment Guide

This guide walks through deploying the complete Ticketmaster data lakehouse to Databricks.

## Prerequisites

1. **Databricks Workspace**
   - Unity Catalog enabled
   - SQL Warehouse (Serverless recommended)
   - Databricks CLI installed

2. **API Access**
   - Ticketmaster API key from [developer.ticketmaster.com](https://developer.ticketmaster.com)

3. **Permissions**
   - CREATE CATALOG permission in Unity Catalog
   - CREATE SCHEMA permission
   - Workspace admin or appropriate job permissions

## Step 1: Configure Databricks CLI

```bash
# Install Databricks CLI
pip install databricks-cli

# Configure authentication
databricks configure --token

# Enter your workspace URL when prompted
# Host: https://fe-vm-tw-vdm-serverless-tixsfe.cloud.databricks.com
# Token: <your-personal-access-token>
```

## Step 2: Set Up Secrets

```bash
# Create secret scope for API keys
databricks secrets create-scope --scope ticketmaster

# Add Ticketmaster API key
databricks secrets put --scope ticketmaster --key api_key

# This will open an editor - paste your API key and save
```

## Step 3: Create Unity Catalog Objects

```bash
# Deploy catalog and schemas
databricks fs cp sql/ddl/create_catalog.sql dbfs:/tmp/
databricks workspace import-dir sql/ddl /Workspace/ticketmaster/sql/ddl
```

Or run directly in SQL Warehouse:

```sql
-- Create catalog
CREATE CATALOG IF NOT EXISTS ticketmaster;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS ticketmaster.bronze;
CREATE SCHEMA IF NOT EXISTS ticketmaster.silver;
CREATE SCHEMA IF NOT EXISTS ticketmaster.gold;

-- Create volume for raw data
CREATE VOLUME IF NOT EXISTS ticketmaster.bronze.raw_data;
```

## Step 4: Upload Notebooks and Code

```bash
# Install Databricks Asset Bundles CLI
pip install databricks-cli

# Navigate to project directory
cd ~/Documents/tix-master

# Validate bundle configuration
databricks bundle validate

# Deploy to development
databricks bundle deploy -t dev

# For production
databricks bundle deploy -t prod
```

## Step 5: Set Up Initial Data Load

### Option A: Using Databricks Workflows UI

1. Navigate to Workflows in Databricks
2. Find job: "[dev] Ticketmaster Daily Ingestion"
3. Click "Run now"
4. Monitor the pipeline execution

### Option B: Using CLI

```bash
# Trigger the ingestion job
databricks jobs run-now --job-name "[dev] Ticketmaster Daily Ingestion"

# Monitor job run
databricks jobs runs list --job-name "[dev] Ticketmaster Daily Ingestion"
```

## Step 6: Verify Deployment

### Check Bronze Layer

```sql
-- Verify Bronze tables exist
SHOW TABLES IN ticketmaster.bronze;

-- Check record counts
SELECT 'events_raw' as table, COUNT(*) as records FROM ticketmaster.bronze.events_raw
UNION ALL
SELECT 'venues_raw', COUNT(*) FROM ticketmaster.bronze.venues_raw
UNION ALL
SELECT 'attractions_raw', COUNT(*) FROM ticketmaster.bronze.attractions_raw;
```

### Check Silver Layer

```sql
-- Verify Silver tables with constraints
SHOW TABLES IN ticketmaster.silver;

-- Verify PK constraints exist
DESCRIBE TABLE EXTENDED ticketmaster.silver.events;

-- Check relationships
SELECT
  e.event_name,
  v.venue_name,
  v.city
FROM ticketmaster.silver.events e
INNER JOIN ticketmaster.silver.event_venues ev ON e.event_id = ev.event_id
INNER JOIN ticketmaster.silver.venues v ON ev.venue_id = v.venue_id
LIMIT 10;
```

### Check Gold Layer

```sql
-- Verify Gold star schema
SHOW TABLES IN ticketmaster.gold;

-- Verify identity columns are working
SELECT
  event_sk,
  event_id,
  event_name
FROM ticketmaster.gold.fact_events
ORDER BY event_sk DESC
LIMIT 10;

-- Test star schema query
SELECT
  d.year,
  d.month_name,
  COUNT(*) as event_count,
  AVG(f.price_max) as avg_price
FROM ticketmaster.gold.fact_events f
INNER JOIN ticketmaster.gold.dim_date d ON f.event_date_key = d.date_key
WHERE d.year = YEAR(CURRENT_DATE())
GROUP BY d.year, d.month_name
ORDER BY d.year, d.month_name;
```

## Step 7: Set Up AI/BI Genie

1. Navigate to **AI/BI** in Databricks workspace
2. Click **Create Genie Space**
3. Configure:
   - Name: "Ticketmaster Events Analytics"
   - SQL Warehouse: Select your warehouse
   - Catalog: `ticketmaster`
   - Schema: `gold`
4. Add instructions from `src/ai/setup_genie.md`
5. Test with sample questions

## Step 8: Deploy RAG Assistant

```bash
# Upload RAG notebook
databricks workspace import src/ai/rag_assistant.py \
  /Workspace/ticketmaster/ai/rag_assistant \
  --language PYTHON
```

Then:
1. Open notebook in Databricks
2. Run all cells to create vector search index
3. Test with sample queries

## Step 9: Schedule Jobs

Jobs are automatically scheduled via Asset Bundles. To modify schedules:

1. Navigate to **Workflows** > **Jobs**
2. Find the job (e.g., "Ticketmaster Daily Ingestion")
3. Click **Edit** > **Schedule**
4. Adjust timing as needed

Or update in `databricks-asset-bundles/resources/jobs.yml` and redeploy.

## Step 10: Monitoring and Alerts

### Set Up Alerts

```sql
-- Create alert for failed data quality checks
-- Navigate to Databricks SQL > Alerts > Create Alert

SELECT
  check_name,
  check_status,
  records_affected
FROM ticketmaster.gold.data_quality_results
WHERE check_run_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 1 DAY
  AND check_status = 'FAILED';
```

### Monitor Job Runs

```bash
# List recent job runs
databricks jobs runs list --limit 10

# Get job run details
databricks jobs runs get --run-id <run-id>
```

### Check ETL Logs

```sql
-- View recent ETL executions
SELECT
  procedure_name,
  start_time,
  end_time,
  rows_processed,
  status
FROM ticketmaster.gold.etl_log
ORDER BY start_time DESC
LIMIT 20;
```

## Troubleshooting

### Issue: API Ingestion Fails

**Check:**
1. API key is correctly set in secrets
2. API rate limits not exceeded
3. Network connectivity from Databricks

```bash
# Test API key
curl "https://app.ticketmaster.com/discovery/v2/events.json?apikey=YOUR_KEY&size=1"
```

### Issue: Auto Loader Not Processing Files

**Check:**
1. Volume permissions
2. Checkpoint locations are accessible
3. Files exist in Volume

```sql
-- List files in volume
LIST '/Volumes/ticketmaster/bronze/raw_data/events/';

-- Check checkpoint
LIST 'dbfs:/tmp/checkpoints/bronze/events/';
```

### Issue: PK/FK Constraint Violations

```sql
-- Find orphaned records
SELECT COUNT(*) as orphaned
FROM ticketmaster.silver.event_venues ev
LEFT JOIN ticketmaster.silver.events e ON ev.event_id = e.event_id
WHERE e.event_id IS NULL;

-- Run data quality checks
CALL ticketmaster.gold.sp_data_quality_checks(@total, @failed, @score, @status);
SELECT @total, @failed, @score, @status;
```

### Issue: Vector Search Index Not Working

1. Check endpoint is running
2. Verify embedding model is available
3. Ensure table has been synced

```python
from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient()
index = vsc.get_index(
    endpoint_name="ticketmaster_vector_search",
    index_name="ticketmaster.gold.events_index"
)
print(index.describe())
```

## Production Deployment

For production deployment:

1. **Update configuration**
   ```bash
   # Edit databricks-asset-bundles/databricks.yml
   # Set production workspace URL and settings
   ```

2. **Create service principal**
   ```bash
   # Use a service principal for production runs
   databricks service-principals create --display-name ticketmaster-prod-sp
   ```

3. **Deploy with production target**
   ```bash
   databricks bundle deploy -t prod
   ```

4. **Set up CI/CD**
   - Use GitHub Actions or Azure DevOps
   - Automate bundle deployments on merge to main
   - Run integration tests before promotion

## Maintenance

### Daily
- Monitor job runs
- Check data quality results
- Review ETL logs

### Weekly
- Review summary reports
- Check storage usage
- Analyze query performance

### Monthly
- Optimize tables (automated via job)
- Vacuum old files (automated via job)
- Update statistics (automated via job)
- Review and adjust schedules

## Next Steps

1. **Connect BI Tools**
   - Tableau, Power BI, or Looker to SQL Warehouse
   - Use Gold layer tables for reporting

2. **Add More Data Sources**
   - Integrate ticket sales data
   - Add customer demographics
   - Include venue capacity information

3. **Enhance Analytics**
   - Predictive models for demand forecasting
   - Recommendation engine for events
   - Price optimization analysis

4. **Expand AI Capabilities**
   - Multi-modal search (images, videos)
   - Sentiment analysis on event reviews
   - Chatbot for customer support

## Support

For issues or questions:
- Check documentation in `/docs`
- Review troubleshooting section above
- Check Databricks community forums
- Contact data engineering team
