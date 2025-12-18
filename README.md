# Ticketmaster Medallion Architecture

A data lakehouse implementation for Ticketmaster API data using Databricks Unity Catalog with Bronze/Silver/Gold medallion architecture, deployed via Databricks Asset Bundles (DAB).

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
- **Design**: Star schema with identity surrogate keys
- **Features**:
  - `fact_events`: Grain = one event occurrence
  - Dimension tables with SCD Type 2 (venue, attraction)
  - Auto-incrementing identity columns for performance
  - Pre-aggregated monthly summaries

## 📊 ETL Pipeline

The pipeline runs as a Databricks Job with the following tasks:

```
Task 0: ingest_ticketmaster_data
   ↓  (Fetch data from API → Save to Volume)
   
Task 1: bronze_auto_loader
   ↓  (Stream JSON → Delta tables)
   
Task 2: silver_transformations
   ↓  (Normalize → Apply constraints)
   
Task 3: data_quality_checks
   ↓  (7 automated validation rules)
   
Task 4: gold_star_schema
   ↓  (Build star schema → MERGE facts)
   
Task 5: generate_event_summary
   ↓  (Monthly KPI aggregations)
```

**Schedule**: Daily at 2 AM PST (configurable in `resources/jobs.yml`)

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

# Run the ETL pipeline
databricks bundle run tix_master_etl_pipeline
```

### 4. Monitor Your Pipeline

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
│   │   └── ticketmaster_ingestion_notebook.py
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
    ├── dim_date               # Date dimension (PK: date_key)
    ├── dim_venue              # Venue dimension (PK: venue_sk) - SCD Type 2
    ├── dim_attraction         # Attraction dimension (PK: attraction_sk) - SCD Type 2
    ├── dim_classification     # Classification dimension (PK: classification_sk)
    ├── dim_market             # Market dimension (PK: market_sk)
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
2. **Open the RAG notebook**: `src/ai/rag/rag_assistant.py`
3. **Execute cells** to:
   - Create event documents table
   - Set up Vector Search endpoint and index
   - Test queries

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

### Maintenance

```sql
-- Refresh vector index when new events are added
ALTER INDEX ticket_master.gold.events_index SYNC;
```

The index can be automatically synced when new data is loaded into the pipeline.

## 📖 Additional Resources

- [Databricks Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
- [Unity Catalog PK/FK Constraints](https://www.databricks.com/blog/primary-key-and-foreign-key-constraints-are-ga-and-now-enable-faster-queries)
- [Databricks Asset Bundles Documentation](https://docs.databricks.com/dev-tools/bundles/)
- [Auto Loader](https://docs.databricks.com/ingestion/auto-loader/index.html)
- [Ticketmaster API Documentation](https://developer.ticketmaster.com/products-and-docs/apis/getting-started/)

## 🤝 Contributing

For detailed architecture documentation, see `docs/ARCHITECTURE.md`.
For deployment strategies, see `DEPLOYMENT.md`.
For secrets setup, see `SECRETS_SETUP.md`.

## 📝 License

This project is for educational and demonstration purposes.
