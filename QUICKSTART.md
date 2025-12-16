# Ticketmaster Medallion Architecture - Quick Start

Get up and running in 15 minutes!

## Prerequisites

1. Databricks workspace: `https://fe-vm-tw-vdm-serverless-tixsfe.cloud.databricks.com`
2. Ticketmaster API key from: https://developer.ticketmaster.com/

## Step 1: Set Up Secrets (2 min)

```bash
# Install Databricks CLI if needed
pip install databricks-cli

# Configure
databricks configure --token
# Host: https://fe-vm-tw-vdm-serverless-tixsfe.cloud.databricks.com
# Token: <your-token>

# Create secrets
databricks secrets create-scope --scope ticketmaster
databricks secrets put --scope ticketmaster --key api_key
# Paste your Ticketmaster API key when prompted
```

## Step 2: Deploy (3 min)

```bash
cd ~/Documents/tix-master

# Install dependencies
pip install -r requirements.txt

# Deploy to Databricks
databricks bundle deploy -t dev
```

## Step 3: Create Catalog & Schemas (2 min)

Open SQL Warehouse and run:

```sql
CREATE CATALOG IF NOT EXISTS ticketmaster;
CREATE SCHEMA IF NOT EXISTS ticketmaster.bronze;
CREATE SCHEMA IF NOT EXISTS ticketmaster.silver;
CREATE SCHEMA IF NOT EXISTS ticketmaster.gold;
CREATE VOLUME IF NOT EXISTS ticketmaster.bronze.raw_data;
CREATE TABLE IF NOT EXISTS ticketmaster.gold.etl_log (
  log_id BIGINT GENERATED ALWAYS AS IDENTITY,
  procedure_name STRING,
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  parameters STRING,
  rows_processed INT,
  status STRING,
  error_message STRING,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
```

## Step 4: Run Initial Load (5 min)

Option A - Via UI:
1. Go to Workflows > Jobs
2. Find "[dev] Ticketmaster Daily Ingestion"
3. Click "Run now"

Option B - Via CLI:
```bash
databricks jobs run-now --job-name "[dev] Ticketmaster Daily Ingestion"
```

## Step 5: Verify (3 min)

```sql
-- Check Bronze
SELECT COUNT(*) FROM ticketmaster.bronze.events_raw;

-- Check Silver
SELECT COUNT(*) FROM ticketmaster.silver.events;

-- Check Gold
SELECT
  d.month_name,
  COUNT(*) as events,
  AVG(f.price_max) as avg_price
FROM ticketmaster.gold.fact_events f
JOIN ticketmaster.gold.dim_date d ON f.event_date_key = d.date_key
GROUP BY d.month_name;
```

## What You Get

### Data Pipeline
✅ Bronze → Silver → Gold processing
✅ Auto Loader streaming
✅ PK/FK constraints
✅ Identity surrogate keys
✅ Data quality checks

### Analytics
✅ Star schema for BI
✅ Materialized views
✅ Stored procedures
✅ Scheduled jobs

### AI Features
✅ AI/BI Genie setup
✅ RAG assistant ready
✅ Vector search index

## Key Files

- **Ingestion**: `src/ingestion/ticketmaster_ingestion.py`
- **Bronze**: `src/bronze/bronze_auto_loader.py`
- **Silver**: `src/silver/silver_transformations.py`
- **Gold**: `src/gold/gold_star_schema.py`
- **Config**: `config/config.yaml`
- **Jobs**: `databricks-asset-bundles/resources/jobs.yml`

## Documentation

- **Architecture**: `docs/ARCHITECTURE.md` - Complete technical design
- **Deployment**: `docs/DEPLOYMENT_GUIDE.md` - Step-by-step setup
- **AI Setup**: `src/ai/setup_genie.md` - Genie configuration

## Troubleshooting

**Job fails?**
```bash
databricks jobs runs list --limit 1
databricks jobs runs get --run-id <id>
```

**Check logs:**
```sql
SELECT * FROM ticketmaster.gold.etl_log ORDER BY start_time DESC LIMIT 10;
```

**Data quality:**
```sql
CALL ticketmaster.gold.sp_data_quality_checks(@total, @failed, @score, @status);
```

## Next Steps

1. **Connect BI tool** to SQL Warehouse
2. **Enable Genie** for natural language queries
3. **Run RAG notebook** for AI assistant
4. **Schedule reports** from materialized views
5. **Set up alerts** on data quality

## Support

- Full docs in `/docs` directory
- Architecture diagram in `docs/ARCHITECTURE.md`
- Deployment guide in `docs/DEPLOYMENT_GUIDE.md`

Happy analyzing! 🎫✨
