# Ticketmaster Medallion Architecture - Setup Guide

Complete setup in 15 minutes by running SQL in the Databricks UI and then executing notebooks.

## 🚀 Quick Setup

### Step 1: Create Infrastructure (5 minutes)

1. **Open SQL Warehouse UI**:
   https://fe-vm-tw-vdm-serverless-tixsfe.cloud.databricks.com/#sql

2. **Create New Query** and run the `setup.sql` file contents:

```sql
-- Create Catalog
CREATE CATALOG IF NOT EXISTS ticketmaster_dev;

-- Create Schemas
CREATE SCHEMA IF NOT EXISTS ticketmaster_dev.bronze;
CREATE SCHEMA IF NOT EXISTS ticketmaster_dev.silver;
CREATE SCHEMA IF NOT EXISTS ticketmaster_dev.gold;

-- Create Volume
CREATE VOLUME IF NOT EXISTS ticketmaster_dev.bronze.raw_data;

-- Create ETL Log Table
CREATE TABLE IF NOT EXISTS ticketmaster_dev.gold.etl_log (
  log_id BIGINT GENERATED ALWAYS AS IDENTITY,
  procedure_name STRING NOT NULL,
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  parameters STRING,
  rows_processed INT,
  status STRING,
  error_message STRING,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Verify Setup
SHOW SCHEMAS IN ticketmaster_dev;
SHOW VOLUMES IN ticketmaster_dev.bronze;
SHOW TABLES IN ticketmaster_dev.gold;
```

Expected output:
```
✓ Catalog: ticketmaster_dev created
✓ Schemas: bronze, silver, gold created
✓ Volume: raw_data created
✓ Table: etl_log created
```

---

### Step 2: Run Notebooks (10 minutes)

Execute notebooks in order. Navigate to:
https://fe-vm-tw-vdm-serverless-tixsfe.cloud.databricks.com/#workspace/Users/tanner.waggoner@databricks.com/tix-master

**Run in this order:**

#### 1. Bronze Layer
**Notebook**: `src/bronze/bronze_auto_loader`
- Attach to **Serverless** compute
- Click **Run All**
- **Creates**: `events_raw`, `venues_raw`, `attractions_raw`, `classifications_raw`

#### 2. Silver Layer
**Notebook**: `src/silver/silver_transformations`
- Attach to **Serverless** compute
- Click **Run All**
- **Creates**:
  - Dimensions: `events`, `venues`, `attractions`, `classifications`, `markets`
  - Bridge tables: `event_venues`, `event_attractions`
  - With PK/FK constraints and liquid clustering

#### 3. Gold Layer
**Notebook**: `src/gold/gold_star_schema`
- Attach to **Serverless** compute
- Click **Run All**
- **Creates**:
  - Fact: `fact_events` (with identity keys)
  - Dimensions: `dim_venue`, `dim_attraction`, `dim_date`, `dim_classification`, `dim_market`
  - Liquid clustering configured
  - Materialized views

---

## ✅ Verification

After running all notebooks, verify in SQL Warehouse:

```sql
-- Check Bronze tables
SHOW TABLES IN ticketmaster_dev.bronze;
SELECT COUNT(*) FROM ticketmaster_dev.bronze.events_raw;

-- Check Silver tables
SHOW TABLES IN ticketmaster_dev.silver;
SELECT COUNT(*) FROM ticketmaster_dev.silver.events;

-- Check Gold tables and query star schema
SHOW TABLES IN ticketmaster_dev.gold;

SELECT
  d.month_name,
  COUNT(*) as event_count,
  AVG(f.price_max) as avg_max_price
FROM ticketmaster_dev.gold.fact_events f
JOIN ticketmaster_dev.gold.dim_date d ON f.event_date_key = d.date_key
WHERE f.is_test = FALSE
GROUP BY d.month_name
ORDER BY d.month_name;
```

---

## 📊 Expected Results

| Layer | Tables | Features |
|-------|--------|----------|
| **Bronze** | 4 tables | Auto Loader, ingestion time clustering |
| **Silver** | 7 tables | PK/FK constraints, liquid clustering |
| **Gold** | 6 tables | Identity keys, star schema, liquid clustering |

---

## 🔧 Troubleshooting

### No Data in Bronze Tables

**Cause**: API ingestion hasn't run yet or no data in Volume

**Solution**:
1. Check Volume has data: `LIST '/Volumes/ticketmaster_dev/bronze/raw_data/'`
2. If empty, you need to run API ingestion first (see `src/ingestion/ticketmaster_ingestion.py`)

### Notebook Execution Failed

**Possible causes**:
- Serverless compute not available → Use a regular cluster
- No permissions → Check Unity Catalog permissions
- Syntax error → Check notebook cell output for details

### Permission Denied

**Solution**: Ensure you have:
- CREATE CATALOG permission
- CREATE SCHEMA permission
- USE CATALOG permission on `ticketmaster_dev`

---

## 🎯 Next Steps

### 1. Install Stored Procedures (Optional)

Run SQL files from `sql/stored_procedures/` in SQL Warehouse:
- `sp_data_quality_checks.sql` - Automated quality checks
- `sp_generate_event_summary.sql` - Monthly reports
- `sp_refresh_gold_layer.sql` - Gold layer refresh

### 2. Enable AI/BI Genie (Optional)

Follow: `src/ai/setup_genie.md`

### 3. Set Up RAG Assistant (Optional)

Run: `src/ai/rag_assistant.py`

### 4. Connect BI Tools

Use Gold layer tables with your BI tool:
- Tableau
- Power BI
- Looker

Connection details available in SQL Warehouse UI.

---

## 📚 Documentation

- **Architecture**: `docs/ARCHITECTURE.md` - Complete technical design
- **Deployment**: `docs/DEPLOYMENT_GUIDE.md` - Detailed deployment
- **Quick Start**: `QUICKSTART.md` - Fast commands reference

---

## 🔗 Workspace Links

- **Workspace Home**: https://fe-vm-tw-vdm-serverless-tixsfe.cloud.databricks.com
- **Your Notebooks**: https://fe-vm-tw-vdm-serverless-tixsfe.cloud.databricks.com/#workspace/Users/tanner.waggoner@databricks.com/tix-master
- **SQL Warehouse**: https://fe-vm-tw-vdm-serverless-tixsfe.cloud.databricks.com/#sql

---

## ✨ You're Done!

You now have a complete Medallion Architecture with:
- ✅ Liquid clustering (2025 best practice)
- ✅ PK/FK constraints with ERD
- ✅ Identity surrogate keys
- ✅ Star schema ready for BI
- ✅ Stored procedures with control flow

Happy analyzing! 🎫
