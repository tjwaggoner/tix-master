# Ticketmaster Medallion Architecture

A data lakehouse implementation for Ticketmaster API data using Databricks Unity Catalog with Bronze/Silver/Gold medallion architecture, deployed via Databricks Asset Bundles (DAB).

## 🚀 Quick Start

### Two-Phase Data Loading Strategy

This pipeline uses a **two-phase approach** for data ingestion:

1. **📥 Historical Ingestion** (One-Time Manual Run)
   - **When**: Run once before starting the scheduled job
   - **What**: Loads 6 months historical + 1 year future events
   - **Where**: `src/ingestion/ticketmaster_historical_ingestion.py`
   - **How**: Open in Databricks workspace UI and click "Run All"
   - **Note**: Historical lookback limited to 6 months due to Ticketmaster API data retention
   
2. **🔄 Incremental Ingestion** (Daily Scheduled Job)
   - **When**: Runs automatically daily at 2 AM PST
   - **What**: Fetches only new/updated data since last run
   - **Where**: `src/ingestion/ticketmaster_incremental_ingestion.py`
   - **How**: Automatically triggered by the scheduled job

> ⚠️ **IMPORTANT**: The historical ingestion is **NOT** part of the scheduled job. Run it manually **ONCE** before starting the daily pipeline.

## 🏗️ Architecture Overview

This project implements a complete data pipeline with three layers:

```
┌─────────────────────────────────────────────────────────────────┐
│                     TICKETMASTER API                            │
│              (Events, Venues, Attractions)                      │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│              INGESTION → Unity Catalog Volumes                  │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  BRONZE (Raw)    → Auto Loader → Delta Tables                  │
│  - events_raw, venues_raw, attractions_raw                      │
│  - Schema inference & evolution                                 │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  SILVER (Normalized) → PK/FK Constraints → 3NF Tables          │
│  - events, venues, attractions, classifications                 │
│  - Bridge tables (event_venues, event_attractions)              │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  GOLD (Star Schema)  → Identity Keys → Analytics Ready         │
│  - fact_events, dim_venue, dim_attraction, dim_date            │
│  - Monthly aggregations (pre-computed KPIs)                     │
└─────────────────────────────────────────────────────────────────┘
```

### Layer Details

#### 🥉 Bronze Layer (Raw Zone)
- **Purpose**: Land raw data with minimal transformation
- **Technology**: Auto Loader (cloudFiles) + Delta Lake
- **Features**:
  - Automatic schema inference and evolution
  - Exactly-once processing guarantees
  - Metadata columns (`_ingestion_timestamp`, `_source_file`)
  - Unity Catalog Volume staging

#### 🥈 Silver Layer (Normalized Zone)
- **Purpose**: Clean, normalized, and validated data
- **Design**: Third Normal Form (3NF) with PK/FK constraints
- **Features**:
  - Primary key constraints on all tables
  - Foreign key constraints for referential integrity
  - Deduplication on natural business keys
  - Data quality validations

#### 🥇 Gold Layer (Star Schema Zone)
- **Purpose**: Consumption-ready dimensional model
- **Design**: Star schema with surrogate keys
- **Features**:
  - `fact_events`: Grain = one event occurrence
  - Dimension tables with SCD Type 2 (venue, attraction, classification, market)
  - Surrogate keys for efficient joins
  - Pre-aggregated monthly summaries

### Slowly Changing Dimensions (SCD Type 2)

The Gold layer implements **SCD Type 2** for all dimensions (venue, attraction, classification, market) to track historical changes over time.

#### How SCD Type 2 Works

Each dimension maintains multiple versions of a record when attributes change:

```sql
-- Example: Venue name changes from "Madison Square Garden" to "MSG Arena"

-- Before change (expired record):
venue_sk: abc123, venue_id: V123, name: "Madison Square Garden"
is_current: FALSE, valid_from: 2024-01-01, valid_to: 2024-06-15

-- After change (current record):
venue_sk: abc123, venue_id: V123, name: "MSG Arena"
is_current: TRUE, valid_from: 2024-06-15, valid_to: NULL
```

#### Key Columns

All SCD Type 2 dimensions include:
- **`valid_from`**: When this version became active (NOT NULL)
- **`valid_to`**: When this version expired (NULL for current records)
- **`is_current`**: Boolean flag (TRUE for active version, FALSE for historical)

#### Primary Keys

**Single-column primary key** on surrogate key to support foreign key constraints:
```sql
PRIMARY KEY (venue_sk)  -- Single column, not composite
```

**Why single-column instead of composite?**
- Allows fact/bridge tables to reference dimension via FK constraints
- Composite PK `(venue_sk, valid_from)` would require fact table to store both columns
- Fact table only stores `venue_sk_fk`, not the temporal component
- Multiple versions per surrogate key are still allowed (SCD Type 2)

**Trade-off**:
- ✅ Enables FK constraints for query optimization and documentation
- ⚠️ Doesn't enforce version uniqueness at PK level
- ⚠️ UNIQUE constraint on `(venue_sk, valid_from)` would be ideal but is disabled in workspace

#### Querying SCD Type 2 Dimensions

**Always filter on `is_current = TRUE`** to get the latest dimension values:

```sql
-- ✅ CORRECT: Filter on is_current
SELECT e.event_name, v.venue_name, v.city
FROM fact_events e
JOIN dim_venue v
  ON e.venue_sk_fk = v.venue_sk
  AND v.is_current = TRUE;

-- ❌ INCORRECT: Missing is_current filter returns duplicate rows
SELECT e.event_name, v.venue_name, v.city
FROM fact_events e
JOIN dim_venue v ON e.venue_sk_fk = v.venue_sk;
```

#### Point-in-Time Queries

To see what the data looked like at a specific date:

```sql
-- What was the venue name on March 1, 2024?
SELECT *
FROM dim_venue
WHERE venue_id = 'V123'
  AND '2024-03-01' >= valid_from
  AND ('2024-03-01' < valid_to OR valid_to IS NULL);
```

#### MERGE Logic

The pipeline uses a two-step process to implement SCD Type 2:

1. **Step 1: Expire Changed Records**
```sql
MERGE INTO dim_venue AS t
USING silver.venues AS s
ON t.venue_id = s.venue_id AND t.is_current = TRUE
WHEN MATCHED AND (attributes_changed) THEN UPDATE SET
  t.is_current = FALSE,
  t.valid_to = current_timestamp();
```

2. **Step 2: Insert New Versions**
```sql
INSERT INTO dim_venue (...)
SELECT ... FROM silver.venues s
LEFT JOIN dim_venue t ON s.venue_id = t.venue_id AND t.is_current = TRUE
WHERE t.venue_id IS NULL  -- New records
   OR (attributes_changed);  -- Changed records
```

#### Benefits
- ✅ Track historical changes to venue names, locations, etc.
- ✅ Audit trail for compliance and debugging
- ✅ Point-in-time analysis capabilities
- ✅ Preserve context for historical events

#### Trade-offs
- ⚠️ More complex queries (must filter on `is_current = TRUE`)
- ⚠️ Larger dimension tables (multiple versions per entity)
- ⚠️ More complex ETL logic

#### Foreign Key Constraints with SCD Type 2

**Good News:** The fact_events table **DOES** have foreign key constraints to all dimensions, including SCD Type 2 dimensions!

**How is this possible with SCD Type 2?**
- Dimension PKs were changed from composite `(venue_sk, valid_from)` to single-column `(venue_sk)`
- This allows fact table to reference dimension using just the surrogate key
- FK constraints are **informational** (use RELY/RELY NOVALIDATE flags)
- They help with query optimization and documentation, but don't enforce referential integrity

**Foreign Key Constraints:**

**fact_events**:
- `date_sk_fk` → `dim_date(date_sk)` RELY
- `venue_sk_fk` → `dim_venue(venue_sk)` RELY NOVALIDATE
- `classification_sk_fk` → `dim_classification(classification_sk)` RELY NOVALIDATE

**bridge_event_attractions**:
- `event_sk_fk` → `fact_events(event_sk)` RELY
- `attraction_sk_fk` → `dim_attraction(attraction_sk)` RELY NOVALIDATE

**Column Comments on FK Fields:**
All FK columns include detailed comments documenting:
- Which table/column they reference
- How the surrogate key is computed (MD5 formula)
- For SCD Type 2 dimensions: "Must filter is_current = TRUE in joins"

**Example:**
```sql
DESCRIBE TABLE ticket_master.gold.fact_events;
-- venue_sk_fk comment:
-- "Foreign key to dim_venue.venue_sk: MD5(venue_id).
--  SCD Type 2: Must filter is_current = TRUE in joins."
```

**Why RELY NOVALIDATE?**
- **RELY**: Tells query optimizer FK relationship exists (improves query plans)
- **NOVALIDATE**: Doesn't enforce referential integrity (allows multiple dimension versions)
- FK constraints are informational/documentary, not enforcement mechanisms
- Referential integrity maintained through ETL logic
- Queries must still use `is_current = TRUE` filter for SCD Type 2 dimensions

## 📊 ETL Pipeline

### Scheduled Job (Daily Incremental Updates)

The pipeline runs as a Databricks Job with the following tasks:

```
Task 0: create_etl_log_table
   ↓  (Create ETL logging table)

Task 1: ingest_ticketmaster_data (INCREMENTAL)
   ↓  (Fetch NEW data from API since last run → Save to Volume)
   
Task 2: bronze_auto_loader
   ↓  (Stream JSON → Delta tables)
   
Task 3: silver_transformations
   ↓  (Normalize → Apply constraints)
   
Task 4: data_quality_checks
   ↓  (7 automated validation rules)
   
Task 5: gold_star_schema
   ↓  (Build star schema → MERGE facts)
   
Task 6: generate_event_summary
   ↓  (Monthly KPI aggregations)
```

**Schedule**: Daily at 2 AM PST (configurable in `resources/jobs.yml`)

### ⚠️ Historical Ingestion (One-Time Manual Run)

**Before running the scheduled job for the first time**, you must manually run the historical ingestion notebook to populate the initial dataset:

```bash
# Navigate to your Databricks workspace UI
# Go to: Workspace → Users → [your-user] → .bundle → tix-master → dev → files → src → ingestion
# Open and run: ticketmaster_historical_ingestion.py
```

**What it does:**
- Fetches 2 years of historical data + 1 year future events
- Loads all venues, attractions, and classifications
- Creates Unity Catalog resources (catalog, schema, volume)
- Writes data to volumes for Bronze layer processing

**Important:**
- ✅ Run this **ONCE** before starting the scheduled job
- ⏭️ **DO NOT** add this to the scheduled job (it fetches too much data)
- 🔄 After this initial load, the scheduled job handles incremental updates

## ✨ Features

- ✅ **Unity Catalog** - Centralized governance and access control
- ✅ **Serverless Compute** - Auto-scaling, zero-management infrastructure
- ✅ **Auto Loader** - Incremental streaming ingestion with schema evolution
- ✅ **PK/FK Constraints** - Enforce data integrity and enable query optimization
- ✅ **Identity Columns** - Auto-incrementing surrogate keys for star schema
- ✅ **Stored Procedures** - Complex SQL logic with control flow (WHILE, IF/ELSE)
- ✅ **Data Quality Checks** - Automated validation with orphaned record detection
- ✅ **SCD Type 2** - Track historical changes in dimension tables
- ✅ **CI/CD** - Databricks Asset Bundles for version-controlled deployments
- ✅ **AI/BI Ready** - Integration with Genie and RAG assistants

## 🚀 Prerequisites

Before deploying, ensure you have:

### 1. Databricks Workspace Requirements
- ✅ **Serverless Compute Enabled** - Required for job execution
- ✅ **Unity Catalog Access** - Permission to create catalogs, schemas, and tables
- ✅ **SQL Warehouse** - Serverless SQL warehouse for stored procedures

### 2. Manual Setup Steps

#### Create Unity Catalog
```sql
-- Run this in Databricks SQL or a notebook
CREATE CATALOG IF NOT EXISTS ticket_master; -- If this doesn't work; Click 'Catalog' -> '+' -> 'Add Catalog' 

-- Grant permissions (optional, for team access)
GRANT USE CATALOG, CREATE SCHEMA ON CATALOG ticket_master TO `your_group`;
```

#### Create Databricks Secret Scope
```bash
# Create secret scope for API credentials
databricks secrets create-scope ticketmaster

# Add your Ticketmaster API key (opens an editor)
databricks secrets put-secret ticketmaster api_key
```

Get your Ticketmaster API key from: https://developer.ticketmaster.com/

#### Get Your SQL Warehouse ID
1. Navigate to **SQL Warehouses** in your Databricks workspace
2. Select your serverless warehouse
3. Copy the **Warehouse ID** from the URL or details page
4. Update `databricks.yml`:
   ```yaml
   variables:
     warehouse_id: "your_warehouse_id_here"
   ```

## 📦 Getting Started

### 1. Set Up Python Environment

```bash
# Clone the repository (or use existing)
cd ~/Documents/tix-master

# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure Databricks CLI

```bash
# Authenticate with Databricks
databricks configure --token

# Enter your workspace URL and personal access token when prompted
# Token: Create in Databricks UI → Settings → User Settings → Access Tokens
```

### 3. Validate and Deploy

```bash
# Validate the bundle configuration
databricks bundle validate

# Deploy all resources to Databricks
databricks bundle deploy
```

### 4. Run Historical Ingestion (One-Time Setup)

**⚠️ IMPORTANT: Run this ONCE before starting the scheduled job**

1. Navigate to your Databricks workspace UI
2. Go to: **Workspace** → **Users** → **[your-user]** → **.bundle** → **tix-master** → **dev** → **files** → **src** → **ingestion**
3. Open `ticketmaster_historical_ingestion.py`
4. Click **Run All** to execute the notebook

This will:
- ✅ Create Unity Catalog resources (catalog, schema, volume)
- ✅ Fetch 2 years of historical events + 1 year future events
- ✅ Load all venues, attractions, and classifications
- ✅ Populate the volumes for Bronze layer processing

**Duration**: 20-30 minutes depending on data volume and API response times.

#### Historical Ingestion Performance Metrics

Based on recent ingestion run:

| Metric                 | Value                                     |
|------------------------|-------------------------------------------|
| **Total Records**      | 12,861 records                            |
| **Events**             | 11,244 records (87.05 MB)                 |
| **Venues**             | 800 records (2.40 MB)                     |
| **Attractions**        | 800 records (3.18 MB)                     |
| **Classifications**    | 17 records (0.47 MB)                      |
| **Total Data Size**    | 93.11 MB                                  |
| **Duration**           | 48.1 seconds (~0.8 minutes)               |
| **Data Throughput**    | **1.94 MB/s** (116 MB/min)                |
| **Record Throughput**  | 267 records/second                        |
| **Storage Location**   | `/Volumes/ticket_master/bronze/raw_data` |

**Key Takeaways for Scaling:**
- The pipeline processes ~2 MB/s of JSON data
- Can handle ~270 records/second end-to-end (API fetch → disk write)
- Total historical load (2 years back + 1 year forward) completes in < 1 minute
- Throughput is primarily limited by Ticketmaster API rate limits, not compute

### 5. Start the Scheduled Pipeline

After the historical ingestion completes, run the incremental ETL pipeline:

```bash
# Run the full ETL pipeline (incremental ingestion + transformations)
databricks bundle run tix_master_etl_pipeline
```

The pipeline will now run daily at 2 AM PST, fetching only new/updated data since the last run.

### 6. Monitor Your Pipeline

View job status in the Databricks UI:
- Navigate to **Workflows**
- Find `[dev] Tix Master ETL Pipeline`
- Click to see run history, logs, and task-level details

Or watch from the command line:
```bash
databricks bundle run tix_master_etl_pipeline --watch
```

## 🗂️ Project Structure

```
tix-master/
├── databricks.yml              # Main DAB configuration
├── resources/
│   └── jobs.yml                # Job and task definitions
├── src/
│   ├── ingestion/              # API ingestion notebooks
│   │   ├── ticketmaster_historical_ingestion.py  # ONE-TIME manual run
│   │   └── ticketmaster_incremental_ingestion.py # Scheduled daily
│   ├── bronze/                 # Bronze layer notebooks
│   │   └── bronze_auto_loader.py
│   ├── silver/                 # Silver layer transformation
│   │   └── silver_transformations.py
│   ├── gold/                   # Gold layer star schema
│   │   └── gold_star_schema.py
│   └── ai/                     # AI/BI integration
│       ├── rag_assistant.py
│       └── setup_genie.md
├── sql/
│   ├── ddl/                    # Table definitions
│   │   └── create_etl_log.sql
│   └── stored_procedures/      # SQL Warehouse procedures
│       ├── sp_data_quality_checks.sql
│       └── sp_generate_event_summary.sql
├── config/
│   └── config.yaml             # Application config
├── requirements.txt            # Python dependencies
└── README.md                   # This file
```

## 🗄️ Unity Catalog Structure

The project automatically creates the following structure:

```
catalog: ticket_master
├── schema: bronze              # Raw data from API
│   ├── raw_data (volume)      # JSON files stored here
│   ├── events_raw             # Raw events table
│   ├── venues_raw             # Raw venues table
│   ├── attractions_raw        # Raw attractions table
│   └── classifications_raw    # Raw classifications table
├── schema: silver             # Normalized relational layer
│   ├── events                 # Core events table (PK: event_id)
│   ├── venues                 # Venues (PK: venue_id)
│   ├── attractions            # Attractions (PK: attraction_id)
│   ├── classifications        # Classifications (PK: classification_id)
│   ├── markets                # Markets (PK: market_id)
│   ├── event_venues           # Bridge table (M:N)
│   └── event_attractions      # Bridge table (M:N)
└── schema: gold               # Star schema for analytics
    ├── dim_date               # Date dimension (PK: date_sk)
    ├── dim_venue              # Venue dimension (PK: venue_sk, valid_from) - SCD Type 2
    ├── dim_attraction         # Attraction dimension (PK: attraction_sk, valid_from) - SCD Type 2
    ├── dim_classification     # Classification dimension (PK: classification_sk, valid_from) - SCD Type 2
    ├── dim_market             # Market dimension (PK: market_sk, valid_from) - SCD Type 2
    ├── fact_events            # Event facts (PK: event_sk)
    ├── monthly_event_summary  # Pre-aggregated monthly KPIs
    └── etl_log                # Execution logs
```

## 🔄 Common Workflows

### Making Changes and Redeploying

```bash
# 1. Edit code in your IDE (e.g., src/gold/gold_star_schema.py)

# 2. Deploy changes
databricks bundle deploy

# 3. Run the updated pipeline
databricks bundle run tix_master_etl_pipeline
```

### Re-running Historical Ingestion

If you need to reload all historical data (e.g., after dropping tables):

1. Navigate to Databricks workspace UI
2. Open `src/ingestion/ticketmaster_historical_ingestion.py`
3. Click **Run All**

**⚠️ Note:** This notebook is **NOT** part of the scheduled job. Only run it manually when needed.

### Running Individual Tasks

```bash
# Test only the bronze layer
databricks bundle run tix_master_bronze_refresh
```

### Viewing Configuration

```bash
# See what will be deployed (dry run)
databricks bundle deploy --dry-run

# View interpolated configuration with all variables resolved
databricks bundle deploy --dry-run --verbose
```

## 🔧 Troubleshooting

### Authentication Issues
```bash
# Re-configure credentials
databricks configure --token

# Test connection
databricks workspace ls /
```

### Validation Errors
```bash
# Check for configuration errors
databricks bundle validate

# View detailed error messages
databricks bundle deploy --debug
```

### Job Failures
1. Check logs in **Databricks UI** → **Workflows** → Job → Run Details
2. Verify secret scope and API key are configured
3. Check Unity Catalog permissions (USE CATALOG, CREATE SCHEMA)
4. Ensure SQL Warehouse ID is correct in `databricks.yml`

## 📚 Key Technologies

- **Databricks Runtime**: Serverless compute
- **Unity Catalog**: Data governance and access control
- **Delta Lake**: ACID-compliant storage with time travel
- **Auto Loader**: Incremental file processing with schema evolution
- **PySpark**: Large-scale data processing
- **SQL**: Transformations and stored procedures
- **Databricks Asset Bundles (DAB)**: Infrastructure as code

## 🎯 Data Quality & Monitoring

### Automated Data Quality Checks
The `sp_data_quality_checks` stored procedure validates:
- Orphaned records (FKs without matching PKs)
- Null checks on required fields
- Referential integrity across layers
- Duplicate detection

Results are logged to `gold.data_quality_results` table.

### ETL Logging
All stored procedure executions are logged to `gold.etl_log`:
- Start/end timestamps
- Rows processed
- Execution status
- Error messages

### Job Monitoring
- Email notifications on failure
- Task-level metrics (duration, data processed)
- Workflow UI for visual monitoring

## 🔐 Security Best Practices

1. **Never commit credentials** to git
2. **Use Databricks Secrets** for API keys
3. **Use personal access tokens** for dev deployments
4. **Use service principals** for staging/prod
5. **Grant least-privilege** access via Unity Catalog

## 🤖 AI/RAG Assistant

The project includes a **RAG (Retrieval Augmented Generation)** assistant that enables natural language queries about events.

### How It Works

```
User Question: "Rock concerts in LA under $100"
       ↓
1. Semantic Search (Vector Search)
   → Finds similar events based on meaning
       ↓
2. Retrieved Context (Top 5 events)
   → Event details with venue, date, price
       ↓
3. LLM Generation (Llama 3.1)
   → Generates natural language answer
       ↓
Answer: "I found 3 rock concerts in LA under $100: ..."
```

### Components

- **Vector Search**: Creates embeddings of event descriptions
  - Endpoint: `ticket_master_vector_search`
  - Embedding Model: `databricks-bge-large-en`
  - Source Table: `gold.event_documents`
  
- **LLM**: Generates responses using Foundation Models
  - Model: `databricks-meta-llama-3-1-70b-instruct`
  - Temperature: 0.7 for conversational responses
  
- **Event Documents**: Combines data from star schema
  - Event name, type, date
  - Venue, location (city, state, country)
  - Attraction, genre
  - Price range

### Running the RAG Assistant

1. **Run the ETL pipeline** to populate star schema tables
2. **Setup Vector Search**: `src/ai/rag/setup_vector_search.py`
   - Creates vector search endpoint and index
   - Includes interactive query widget for asking questions
   - Use the widget or call functions directly in the notebook

### Example Queries

```python
# Natural language questions
ask_event_assistant("What concerts are happening in Los Angeles next weekend?")
ask_event_assistant("Are there any sports events in New York in December?")
ask_event_assistant("Show me rock concerts with tickets under $100")
ask_event_assistant("What are the most popular venues for music events?")
```

### Interactive Mode

The notebook includes a widget for interactive querying:
- Enter question in the widget
- Get formatted response with relevant event details
- No SQL knowledge required!

### Using the Assistant

Open `src/ai/rag/setup_vector_search.py` and use the interactive widget:
- Enter questions in natural language
- Get AI-generated answers with event details
- No code required!

### Maintenance

The vector index automatically syncs after each ETL run via the `sync_vector_index` task in the pipeline.

## 📖 Documentation

Comprehensive documentation is available in the [`docs/`](docs/) directory:

### 🚀 Getting Started
- **[Setup & Secrets](docs/setup/SECRETS_SETUP.md)** - Configure Databricks secrets and API keys
- **[Deployment Guide](docs/setup/DEPLOYMENT.md)** - Deploy using Databricks Asset Bundles

### 🏗️ Architecture & Design
- **[Surrogate Keys](docs/SURROGATE_KEYS.md)** - Understanding surrogate key design patterns
- **[Schema Retention](docs/architecture/SCHEMA_RETENTION_ANALYSIS.md)** - Data retention analysis
- **[Pipeline Architecture](docs/architecture/databricks-pipeline-ebook.md)** - Complete medallion architecture guide

### 🤖 AI & Analytics
- **[Databricks Genie](docs/databricks-genie.md)** - AI-powered natural language analytics
- **[Genie Setup](docs/setup_genie.md)** - Quick Genie Space configuration
- **[Lakeview Dashboards](docs/lakeview-dashboard.md)** - Dashboard creation guide

### 🔌 API Integration
- **[Ticketmaster API](docs/api/API_INFO.md)** - API documentation and usage

**📋 [Full Documentation Index →](docs/README.md)**

### External Resources
- [Databricks Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
- [Unity Catalog PK/FK Constraints](https://www.databricks.com/blog/primary-key-and-foreign-key-constraints-are-ga-and-now-enable-faster-queries)
- [Databricks Asset Bundles Documentation](https://docs.databricks.com/dev-tools/bundles/)
- [Auto Loader](https://docs.databricks.com/ingestion/auto-loader/index.html)
- [Ticketmaster API Documentation](https://developer.ticketmaster.com/products-and-docs/apis/getting-started/)

## 🤝 Contributing

Contributions are welcome! Please:
1. Review the [documentation](docs/) first
2. Follow existing code patterns and conventions
3. Update relevant documentation with your changes
4. Test thoroughly before submitting

## 📝 License

This project is for educational and demonstration purposes.
