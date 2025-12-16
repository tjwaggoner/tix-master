# Ticketmaster Medallion Architecture - Technical Architecture

## Overview

This project implements a comprehensive data lakehouse for Ticketmaster event data using Databricks' Medallion Architecture pattern. The system ingests data from public Ticketmaster APIs, processes it through Bronze/Silver/Gold layers, and enables both traditional BI analytics and AI-powered natural language queries.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                     TICKETMASTER API                            │
│              (Events, Venues, Attractions)                      │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                  INGESTION LAYER                                │
│   Python Script → Pagination → JSON Landing                    │
│                 Unity Catalog Volumes                           │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                  BRONZE LAYER (Raw)                             │
│   Auto Loader (cloudFiles) → Delta Tables                      │
│   - events_raw, venues_raw, attractions_raw                     │
│   - Schema inference & evolution                                │
│   - Partitioned by ingestion_date                               │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                  SILVER LAYER (Normalized)                      │
│   Structured Tables with PK/FK Constraints                      │
│   ┌──────────────┐  ┌───────────────┐  ┌──────────────┐       │
│   │   Events     │  │    Venues     │  │  Attractions │       │
│   │  (PK: id)    │  │  (PK: id)     │  │  (PK: id)    │       │
│   └──────┬───────┘  └───────┬───────┘  └──────┬───────┘       │
│          │                   │                  │               │
│          └───────┬───────────┴──────────────────┘               │
│                  │                                               │
│          ┌───────▼────────────┐  ┌──────────────────┐          │
│          │  event_venues      │  │ event_attractions│          │
│          │ (Bridge Tables)    │  │ (Many-to-Many)   │          │
│          └────────────────────┘  └──────────────────┘          │
│                                                                  │
│   - Deduplication on natural keys                              │
│   - Data quality validation                                     │
│   - ERD visualization in Unity Catalog                         │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                  GOLD LAYER (Star Schema)                       │
│                                                                  │
│              ┌─────────────────────────┐                        │
│              │    fact_events          │                        │
│              │  (Grain: 1 event)       │                        │
│              │  - event_sk (IDENTITY)  │                        │
│              │  - event_date_key (FK)  │                        │
│              │  - venue_sk (FK)        │                        │
│              │  - attraction_sk (FK)   │                        │
│              │  - price_min/max        │                        │
│              └────┬─────┬─────┬────┬───┘                        │
│                   │     │     │    │                            │
│      ┌────────────┘     │     │    └──────────────┐            │
│      │                  │     │                    │            │
│  ┌───▼──────┐   ┌───────▼─┐  ┌▼────────┐  ┌──────▼─────┐     │
│  │dim_venue │   │dim_attr │  │dim_date │  │dim_class   │     │
│  │ (venue_sk│   │(attr_sk)│  │(date_key│  │(class_sk)  │     │
│  │ IDENTITY)│   │IDENTITY │  │ natural)│  │ IDENTITY)  │     │
│  └──────────┘   └─────────┘  └─────────┘  └────────────┘     │
│                                                                  │
│  - Identity surrogate keys for performance                     │
│  - Foreign key constraints                                      │
│  - Materialized views for aggregations                         │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                  CONSUMPTION LAYER                              │
│                                                                  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐        │
│  │ SQL Warehouse│  │   AI/BI      │  │     RAG      │        │
│  │   Queries    │  │   Genie      │  │  Assistant   │        │
│  │              │  │              │  │              │        │
│  │  - BI Tools  │  │ - Natural    │  │ - Vector     │        │
│  │  - Reports   │  │   Language   │  │   Search     │        │
│  │  - Dashboards│  │ - Auto SQL   │  │ - LLM Gen    │        │
│  └──────────────┘  └──────────────┘  └──────────────┘        │
└─────────────────────────────────────────────────────────────────┘
```

## Layer Details

### Bronze Layer (Raw Zone)

**Purpose**: Land raw data with minimal transformation

**Technology Stack**:
- Auto Loader (cloudFiles) for incremental ingestion
- Delta Lake for ACID transactions
- Unity Catalog Volumes for file staging

**Key Features**:
- Automatic schema inference and evolution
- Exactly-once processing guarantees
- Metadata columns (_ingestion_timestamp, _source_file)
- Partitioned by ingestion date for efficient queries

**Tables**:
- `bronze.events_raw`
- `bronze.venues_raw`
- `bronze.attractions_raw`
- `bronze.classifications_raw`

### Silver Layer (Normalized Zone)

**Purpose**: Clean, normalized, and validated data

**Design Principles**:
- Third Normal Form (3NF) for data integrity
- Primary key constraints on all dimension tables
- Foreign key constraints for referential integrity
- Deduplication on natural business keys

**Entity Relationship Design**:

```
events (1) ─────< event_venues >───── (N) venues
  │
  └────────────< event_attractions >───── (N) attractions
  │
  └──────────────────> classifications
  │
  └──────────────────> markets
```

**Data Quality**:
- NOT NULL constraints on primary keys
- Check constraints on data ranges
- Orphaned record detection
- Duplicate prevention

**Tables**:
- Core entities: `events`, `venues`, `attractions`, `classifications`, `markets`
- Bridge tables: `event_venues`, `event_attractions`

### Gold Layer (Star Schema Zone)

**Purpose**: Consumption-ready dimensional model

**Design Pattern**: Star Schema

**Fact Table**:
- `fact_events`: Grain = one event occurrence
- Contains measures (prices, counts)
- Foreign keys to all dimensions using identity surrogate keys

**Dimension Tables**:
- `dim_venue`: Venue attributes (SCD Type 2)
- `dim_attraction`: Performer/team details (SCD Type 2)
- `dim_date`: Complete date dimension with attributes
- `dim_classification`: Genre/segment hierarchy
- `dim_market`: Geographic markets

**Identity Columns**:
All dimension tables use `GENERATED ALWAYS AS IDENTITY` for surrogate keys:
- Auto-incrementing, unique, non-null
- Improved query performance via push-down filters
- Smaller join keys vs. composite natural keys

**Materialized Views**:
- `mv_events_by_date_venue`: Daily event counts by location
- `mv_events_by_attraction`: Performance metrics per artist
- `mv_monthly_summary`: High-level monthly KPIs

## Technology Stack

### Databricks Platform
- **Databricks Runtime**: 14.3.x LTS
- **Unity Catalog**: Centralized governance
- **SQL Warehouse**: Serverless compute for queries
- **Auto Loader**: Streaming ingestion
- **Delta Lake**: ACID-compliant storage format

### Languages & Libraries
- **Python**: API ingestion, orchestration
- **SQL**: Transformations, stored procedures
- **PySpark**: Large-scale data processing

### AI/ML Components
- **AI/BI Genie**: Natural language query interface
- **Vector Search**: Semantic search over events
- **Foundation Models**:
  - `databricks-bge-large-en`: Embeddings
  - `databricks-meta-llama-3-1-70b-instruct`: Text generation

## Data Flow

### 1. Ingestion (Daily at 2 AM)

```python
API → Pagination → JSON files → UC Volume
↓
/Volumes/ticketmaster/bronze/raw_data/events/2024/01/15/events_20240115_020000.json
```

### 2. Bronze Processing

```
Auto Loader (streaming)
↓
Read JSON → Infer Schema → Add Metadata → Write Delta
↓
Checkpoint: /tmp/checkpoints/bronze/events
```

### 3. Silver Transformation

```
Explode nested structures → Normalize → Deduplicate → Apply constraints
↓
events, venues, attractions (with PK/FK)
```

### 4. Gold Aggregation

```
Join Silver tables → Create surrogate keys → Build fact table → Compute dimensions
↓
Star schema with identity keys
```

### 5. AI/BI Enablement

```
Event descriptions → Embeddings → Vector Index
                                     ↓
User query → Semantic search → Retrieve context → LLM generation
```

## Stored Procedures

### sp_refresh_gold_layer
- Merges new/updated dimensions (SCD Type 2)
- Inserts new facts
- Handles incremental updates
- Optimizes tables post-load

### sp_generate_event_summary
- Loops through months
- Calculates KPIs per month
- Identifies top venues/attractions
- Demonstrates control flow (WHILE, IF/ELSE)

### sp_data_quality_checks
- 7 automated validation rules
- Orphaned record detection
- Referential integrity checks
- Returns quality score

## Optimization Strategies

### Storage Optimization
- **OPTIMIZE**: Compaction of small files
- **Z-ORDER**: Multi-dimensional clustering on fact table
  ```sql
  OPTIMIZE fact_events ZORDER BY (event_date_key, venue_sk);
  ```
- **VACUUM**: Remove old file versions

### Query Optimization
- **Statistics**: Column statistics for cost-based optimizer
  ```sql
  ANALYZE TABLE fact_events COMPUTE STATISTICS FOR ALL COLUMNS;
  ```
- **Identity Keys**: Push-down predicates, smaller joins
- **Materialized Views**: Pre-aggregated metrics
- **Partition Pruning**: Date-based partitioning

### Streaming Optimization
- **trigger(availableNow=True)**: Batch streaming mode
- **Checkpointing**: Exactly-once processing
- **Schema evolution**: Handle API changes automatically

## Security & Governance

### Unity Catalog
- **Three-level namespace**: catalog.schema.table
- **Centralized permissions**: GRANT/REVOKE
- **Audit logging**: All access tracked
- **Data lineage**: Automatic tracking

### Access Control
```sql
-- Read-only for analysts
GRANT SELECT ON SCHEMA ticketmaster.gold TO `data_analysts`;

-- Write access for ETL
GRANT ALL PRIVILEGES ON SCHEMA ticketmaster.bronze TO `etl_service_principal`;
```

### Secrets Management
- API keys stored in Databricks Secrets
- Referenced as: `{{secrets/ticketmaster/api_key}}`
- Scoped to specific users/groups

## Monitoring & Observability

### ETL Logging
- `etl_log` table tracks all procedure executions
- Captures: start/end time, rows processed, status, errors

### Data Quality Tracking
- `data_quality_results` table logs all checks
- Severity levels: CRITICAL, HIGH, MEDIUM, LOW
- Automated alerts on failures

### Job Monitoring
- Workflow UI shows task-level status
- Email notifications on failure
- Metrics: duration, cluster utilization, costs

## Scalability Considerations

### Current Scale
- ~10K-100K events per day
- ~5K venues
- ~20K attractions
- Gold fact table: millions of rows

### Growth Strategy
1. **Horizontal scaling**: Increase cluster workers
2. **Liquid clustering**: Replace partitioning at large scale
3. **Photon acceleration**: Enable for 3-5x speedup
4. **Serverless compute**: Auto-scaling SQL Warehouse
5. **Incremental processing**: Only process changed data

## Cost Optimization

### Compute
- Spot instances for batch jobs
- Serverless for interactive queries
- Auto-termination for idle clusters

### Storage
- Delta Lake compression
- VACUUM to remove old versions
- Lifecycle policies on UC Volumes

### Query
- Cached results for repeated queries
- Materialized views for expensive aggregations
- Query result caching in SQL Warehouse

## Future Enhancements

1. **Real-time streaming**: Kafka/Kinesis for sub-minute latency
2. **Predictive analytics**: Demand forecasting models
3. **Graph analytics**: Event recommendation engine
4. **Multi-region**: Deploy to multiple clouds
5. **Data mesh**: Domain-oriented decentralized ownership
6. **Change data capture**: Track dimension changes over time
7. **Data quality framework**: Great Expectations integration
8. **Advanced AI**: Fine-tuned models for event classification

## References

- [Databricks Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
- [Unity Catalog PK/FK Constraints](https://www.databricks.com/blog/primary-key-and-foreign-key-constraints-are-ga-and-now-enable-faster-queries)
- [Identity Columns](https://docs.databricks.com/sql/language-manual/sql-ref-syntax-ddl-create-table-using.html#identity-columns)
- [Auto Loader](https://docs.databricks.com/ingestion/auto-loader/index.html)
- [Databricks Vector Search](https://docs.databricks.com/generative-ai/vector-search.html)
- [Ticketmaster API](https://developer.ticketmaster.com/products-and-docs/apis/getting-started/)
