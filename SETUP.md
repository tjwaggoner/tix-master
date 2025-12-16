# Ticketmaster Medallion Architecture - Complete Setup Guide

This guide will help you set up the entire Ticketmaster data lakehouse infrastructure in Databricks.

## 🚀 Quick Setup (5 minutes)

### Option 1: Automated Setup (Bash)

```bash
cd ~/Documents/tix-master

# Activate virtual environment
source venv/bin/activate

# Run setup script
./setup_databricks.sh dev
```

### Option 2: Automated Setup (Python)

```bash
cd ~/Documents/tix-master
source venv/bin/activate

# Setup only
python setup_and_run.py --env dev

# Setup + guidance for running notebooks
python setup_and_run.py --env dev --run-notebooks
```

---

## 📋 What the Setup Scripts Do

Both scripts automatically create:

✅ **Unity Catalog Objects**
- Catalog: `ticketmaster_dev` (or `ticketmaster` for prod)
- Schemas: `bronze`, `silver`, `gold`
- Volume: `ticketmaster_dev.bronze.raw_data`

✅ **Infrastructure Tables**
- `ticketmaster_dev.gold.etl_log` - Tracks stored procedure executions

✅ **Stored Procedures**
- `sp_data_quality_checks` - 7 automated quality checks
- `sp_generate_event_summary` - Monthly summary reports
- `sp_refresh_gold_layer` - Gold layer refresh with SCD Type 2

---

## 🔐 Prerequisites

### 1. Databricks Authentication (Already Done ✓)

Your profile is configured:
```bash
databricks auth profiles
# Shows: tix-master profile configured
```

### 2. Ticketmaster API Key

Get your API key from https://developer.ticketmaster.com/

```bash
# Set API key in Databricks secrets
databricks secrets put --scope ticketmaster --key api_key --profile tix-master
```

When prompted, paste your API key and save.

---

## 📊 Running the Data Pipeline

After setup completes, you need to run the notebooks to create tables and ingest data.

### Step 1: Bronze Layer (Raw Data Ingestion)

Navigate to:
```
https://fe-vm-tw-vdm-serverless-tixsfe.cloud.databricks.com/#workspace/Users/tanner.waggoner@databricks.com/tix-master/src/bronze/bronze_auto_loader
```

1. Click the notebook
2. Attach to **Serverless** compute
3. Click **Run All**

**Creates**: `events_raw`, `venues_raw`, `attractions_raw`, `classifications_raw` tables

### Step 2: Silver Layer (Normalized Tables)

Navigate to:
```
https://fe-vm-tw-vdm-serverless-tixsfe.cloud.databricks.com/#workspace/Users/tanner.waggoner@databricks.com/tix-master/src/silver/silver_transformations
```

1. Click the notebook
2. Attach to **Serverless** compute
3. Click **Run All**

**Creates**:
- `events`, `venues`, `attractions`, `classifications`, `markets` (dimensions)
- `event_venues`, `event_attractions` (bridge tables)
- All with PK/FK constraints
- Liquid clustering enabled

### Step 3: Gold Layer (Star Schema)

Navigate to:
```
https://fe-vm-tw-vdm-serverless-tixsfe.cloud.databricks.com/#workspace/Users/tanner.waggoner@databricks.com/tix-master/src/gold/gold_star_schema
```

1. Click the notebook
2. Attach to **Serverless** compute
3. Click **Run All**

**Creates**:
- `fact_events` (with identity surrogate keys)
- `dim_venue`, `dim_attraction`, `dim_date`, `dim_classification`, `dim_market`
- Liquid clustering configured
- Materialized views for common aggregations

---

## ✅ Verification

### Check Tables Were Created

```bash
# List Bronze tables
databricks sql execute \
  --warehouse-id f4040a30fe978741 \
  --statement 'SHOW TABLES IN ticketmaster_dev.bronze' \
  --profile tix-master

# List Silver tables
databricks sql execute \
  --warehouse-id f4040a30fe978741 \
  --statement 'SHOW TABLES IN ticketmaster_dev.silver' \
  --profile tix-master

# List Gold tables
databricks sql execute \
  --warehouse-id f4040a30fe978741 \
  --statement 'SHOW TABLES IN ticketmaster_dev.gold' \
  --profile tix-master
```

### Check Data

```sql
-- In SQL Warehouse (https://fe-vm-tw-vdm-serverless-tixsfe.cloud.databricks.com/#sql)

-- Check Bronze
SELECT COUNT(*) FROM ticketmaster_dev.bronze.events_raw;

-- Check Silver
SELECT COUNT(*) FROM ticketmaster_dev.silver.events;

-- Check Gold
SELECT
  d.month_name,
  COUNT(*) as events,
  AVG(f.price_max) as avg_price
FROM ticketmaster_dev.gold.fact_events f
JOIN ticketmaster_dev.gold.dim_date d ON f.event_date_key = d.date_key
GROUP BY d.month_name
ORDER BY d.month_name;
```

---

## 🎯 Expected Results

After completing all steps, you should have:

| Layer | Tables | Records (approx) |
|-------|--------|-----------------|
| **Bronze** | 4 tables | 10K-100K total |
| **Silver** | 7 tables | Normalized & deduplicated |
| **Gold** | 6 tables | Star schema ready |

---

## 🔧 Troubleshooting

### Issue: "Warehouse not found"

**Solution**: The warehouse ID might have changed. Find yours:
```bash
databricks warehouses list --profile tix-master
```
Update the warehouse ID in both setup scripts.

### Issue: "Notebook execution failed"

**Possible causes**:
1. **No data in Volume**: Run API ingestion first
2. **Serverless not available**: Use a regular cluster instead
3. **Permissions**: Ensure you have CREATE TABLE permissions

**Solution**: Check the notebook error output and adjust accordingly.

### Issue: "API rate limit exceeded"

**Solution**: The Ticketmaster API has rate limits. Wait a few minutes and try again.

### Issue: "Stored procedures not found"

**Solution**: Run them manually via SQL Warehouse:
1. Go to SQL Warehouse UI
2. Open each SQL file from `/tix-master/sql/stored_procedures/`
3. Replace `ticketmaster` with your catalog name
4. Execute the SQL

---

## 🚀 Next Steps

After setup is complete:

### 1. Enable AI/BI Genie

Follow: `src/ai/setup_genie.md`

### 2. Set Up RAG Assistant

Run: `src/ai/rag_assistant.py`

### 3. Create Schedules (Optional)

Set up jobs to run notebooks on a schedule using Databricks Jobs UI.

### 4. Connect BI Tools

Connect Tableau, Power BI, or Looker to your SQL Warehouse using the Gold layer tables.

---

## 📚 Additional Resources

- **Architecture**: See `docs/ARCHITECTURE.md` for complete technical design
- **Deployment**: See `docs/DEPLOYMENT_GUIDE.md` for detailed deployment instructions
- **Quick Reference**: See `QUICKSTART.md` for fast commands

---

## 🆘 Need Help?

- Check error logs in notebooks
- Review `docs/DEPLOYMENT_GUIDE.md` troubleshooting section
- Verify permissions in Unity Catalog
- Check SQL Warehouse is running

Happy building! 🎫✨
