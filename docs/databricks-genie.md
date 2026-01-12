# Databricks Genie: AI-Powered Analytics Assistant

## Overview

Databricks Genie is an AI-powered conversational analytics assistant that enables natural language interaction with your data. It translates plain English questions into SQL queries, executes them, and presents results with visualizations - making data exploration accessible to both technical and non-technical users.

## How Genie Works

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     User Question                            │
│           "Show me events in the last 7 days"               │
└───────────────────┬─────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────────────────┐
│              Genie AI Engine (LLM)                           │
│  • Understands natural language                             │
│  • Analyzes Unity Catalog metadata                          │
│  • Considers table schemas, column types, comments          │
│  • Infers user intent and context                           │
└───────────────────┬─────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────────────────┐
│           SQL Query Generation                               │
│  SELECT event_name, event_date, venue_name                  │
│  FROM ticket_master.gold.fact_events f                      │
│  JOIN ticket_master.gold.dim_venue v                        │
│    ON f.venue_sk_fk = v.venue_sk AND v.is_current = TRUE   │
│  WHERE event_date >= CURRENT_DATE - INTERVAL 7 DAYS        │
└───────────────────┬─────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────────────────┐
│         SQL Warehouse Execution                              │
│  • Executes generated SQL                                   │
│  • Returns results                                          │
│  • Applies security (row/column filters)                   │
└───────────────────┬─────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────────────────┐
│         Results + Visualization                              │
│  • Table of results                                         │
│  • Auto-generated charts                                    │
│  • Natural language summary                                 │
│  • Suggested follow-up questions                            │
└─────────────────────────────────────────────────────────────┘
```

### Key Components

#### 1. **Natural Language Understanding**
- Genie uses large language models (LLMs) to understand user questions
- Interprets context, temporal references, aggregations, and filters
- Handles ambiguous queries by asking clarifying questions

#### 2. **Unity Catalog Integration**
Genie leverages Unity Catalog metadata:
- **Table schemas**: Column names, data types, constraints
- **Column comments**: Descriptions that guide query generation
- **Table comments**: Business context and usage guidelines
- **Primary/Foreign keys**: Understanding relationships between tables
- **Sample data**: Helps understand data patterns and values

**Example: How Comments Help**
```sql
-- Without comments:
"Show me events by classification"
→ Genie might guess incorrectly

-- With comments on classification_sk_fk:
-- "Foreign key to dim_classification. SCD Type 2: Must filter is_current = TRUE"
→ Genie generates:
   JOIN dim_classification c ON f.classification_sk_fk = c.classification_sk
   AND c.is_current = TRUE
```

#### 3. **SQL Generation**
- Creates syntactically correct SQL for your data model
- Handles:
  - Complex joins (including SCD Type 2 patterns)
  - Aggregations (SUM, COUNT, AVG, etc.)
  - Filtering and WHERE clauses
  - Date/time calculations
  - Window functions
  - CTEs for complex logic

#### 4. **Query Execution**
- Runs on specified SQL Warehouse
- Respects all Unity Catalog security:
  - Row-level security filters
  - Column-level masking
  - Grant-based access control
- Results cached for follow-up questions

#### 5. **Intelligent Visualization**
- Automatically suggests visualizations based on data:
  - Time series → Line charts
  - Categories → Bar/pie charts
  - Distributions → Histograms
  - Geographic data → Maps (with lat/long)
  - Correlations → Scatter plots

#### 6. **Conversational Memory**
- Maintains context across questions
- Allows follow-up refinements:
  - "Show me events in California"
  - "Now filter to just concerts"
  - "Sort by ticket price"

## Setting Up Genie

### Prerequisites

1. **Unity Catalog enabled**
2. **SQL Warehouse** (Serverless recommended)
3. **Proper permissions**:
   - `USE CATALOG` on catalog
   - `USE SCHEMA` on schemas
   - `SELECT` on tables/views
   - `USE` on SQL Warehouse

### Creating a Genie Space

A **Genie Space** is a configured environment that connects to specific:
- Catalog and schema(s)
- SQL Warehouse
- Set of instructions/context

**Steps:**
1. Navigate to **SQL** → **Genie** in Databricks workspace
2. Click **Create Genie Space**
3. Configure:
   - **Name**: "Ticketmaster Events Analytics"
   - **Description**: Purpose and intended users
   - **Data Sources**: Select catalog.schema (e.g., `ticket_master.gold`)
   - **SQL Warehouse**: Choose warehouse
   - **Instructions**: Optional guidance for Genie

### Best Practices for Genie Spaces

#### 1. **Add Comprehensive Instructions**
```markdown
# Ticketmaster Events Data

## Data Model
This is a dimensional star schema with:
- fact_events: Main events table (grain: one row per event)
- dim_venue, dim_attraction, dim_classification: SCD Type 2 dimensions
- bridge_event_attractions: Many-to-many between events and attractions

## Important Patterns
- When joining to SCD Type 2 dimensions (venue, attraction, classification),
  ALWAYS filter by is_current = TRUE
- Date fields: Use event_date for filtering, event_datetime for precise times
- Test events: Filter by is_test = FALSE for production data

## Common Questions
- "Show me upcoming events" → Use event_date >= CURRENT_DATE
- "Events by genre" → Join through dim_classification
- "Which venues have most events" → Count by dim_venue
```

#### 2. **Enrich Table/Column Comments**
```sql
-- Good column comment (helps Genie):
COMMENT ON COLUMN fact_events.venue_sk_fk IS
'Foreign key to dim_venue.venue_sk: MD5(venue_name || latitude || longitude).
SCD Type 2: Must filter is_current = TRUE in joins.';

-- Poor column comment (doesn't help):
COMMENT ON COLUMN fact_events.venue_sk_fk IS 'Venue foreign key';
```

#### 3. **Curate Data Scope**
- Point Genie to gold/silver layers, not bronze
- Include only relevant tables
- Create views to simplify complex queries

#### 4. **Use Descriptive Names**
- `fact_events` > `events_f`
- `event_date` > `dt`
- Clear column names reduce ambiguity

## Using Genie

### Example Questions

#### Basic Queries
```
"Show me all events in the next 30 days"
"How many events are scheduled for each month?"
"What are the top 10 venues by event count?"
```

#### Aggregations
```
"What's the average ticket price by classification?"
"Total events per state"
"Count of events by day of week"
```

#### Joins and Relationships
```
"Show me all Taylor Swift events with venue details"
"Which attractions perform most frequently?"
"Events in California with ticket prices over $100"
```

#### Time-Based Analysis
```
"Compare this month's events to last month"
"Show weekly event trends for the past 6 months"
"What days of the week have the most events?"
```

#### Geographic Queries
```
"Show me events in New York"
"Map all venues in California"
"Events within 50 miles of Los Angeles"
```

### Tips for Better Results

#### 1. **Be Specific**
- ❌ "Show me events"
- ✅ "Show me upcoming concerts in California with ticket prices"

#### 2. **Use Table/Column Names**
- ❌ "Show classifications"
- ✅ "Show me all events grouped by classification segment_name"

#### 3. **Specify Time Ranges**
- ❌ "Show events"
- ✅ "Show events in the next 7 days"

#### 4. **Iterate and Refine**
- Start broad: "Show me events by state"
- Refine: "Now filter to just concerts"
- Add detail: "Sort by event date"

#### 5. **Ask for Explanations**
- "Show me the SQL query you generated"
- "Explain the results"
- "Why did you join these tables?"

## Understanding Genie's Limitations

### What Genie Can Do
✅ Generate SQL queries from natural language
✅ Handle complex joins and aggregations
✅ Create visualizations automatically
✅ Understand business context from comments
✅ Handle SCD Type 2 patterns (with proper comments)
✅ Execute on serverless or classic SQL warehouses

### What Genie Cannot Do
❌ Modify data (INSERT, UPDATE, DELETE)
❌ Create tables or schemas
❌ Access external data sources directly
❌ Execute Python/Scala code
❌ Perform ML model training
❌ Access data outside granted permissions

### Common Issues

#### 1. **Wrong Results**
**Cause**: Missing context or ambiguous question
**Solution**:
- Add table/column comments
- Be more specific in question
- Check generated SQL and refine

#### 2. **SCD Type 2 Joins Missing is_current Filter**
**Cause**: Missing column comments about SCD Type 2
**Solution**: Add detailed FK comments:
```sql
COMMENT ON COLUMN fact_events.venue_sk_fk IS
'FK to dim_venue.venue_sk. SCD Type 2: Filter is_current = TRUE';
```

#### 3. **Slow Queries**
**Cause**:
- Large result sets
- Missing filters
- Undersized warehouse

**Solution**:
- Add date range filters
- Use faster/larger SQL warehouse
- Create aggregated views for common queries

#### 4. **"Table not found" Errors**
**Cause**:
- Genie Space not configured for catalog/schema
- Missing permissions

**Solution**:
- Update Genie Space data sources
- Grant SELECT permissions

## Best Practices for Data Modeling with Genie

### 1. **Comment Everything**
```sql
-- Table comment
COMMENT ON TABLE fact_events IS
'Fact table for ticketed events. One row per event.
Join to dimensions using is_current = TRUE for SCD Type 2.';

-- Column comments
COMMENT ON COLUMN fact_events.event_sk IS 'Primary key: MD5(event_name || event_datetime)';
COMMENT ON COLUMN fact_events.event_date IS 'Event date (DATE type). Use for filtering by date.';
COMMENT ON COLUMN fact_events.price_max IS 'Maximum ticket price in USD. Null if not available.';
```

### 2. **Use Semantic Column Names**
```sql
-- Good
event_date, venue_name, price_min, is_test

-- Bad
dt, vnm, pr1, tst
```

### 3. **Create Business-Friendly Views**
```sql
CREATE VIEW gold.vw_upcoming_events AS
SELECT
  e.event_name,
  e.event_date,
  v.venue_name,
  v.city,
  v.state,
  c.segment_name as genre,
  e.price_min,
  e.price_max
FROM fact_events e
JOIN dim_venue v ON e.venue_sk_fk = v.venue_sk AND v.is_current = TRUE
JOIN dim_classification c ON e.classification_sk_fk = c.classification_sk AND c.is_current = TRUE
WHERE e.event_date >= CURRENT_DATE
  AND e.is_test = FALSE;

COMMENT ON VIEW gold.vw_upcoming_events IS
'Business-friendly view of upcoming events with venue and classification details.
Pre-filtered for production events only (is_test = FALSE).';
```

### 4. **Document Complex Patterns**
Add Genie Space instructions for:
- Bridge table patterns
- SCD Type 2 joins
- Calculated metrics
- Common filters (is_test = FALSE)
- Business rules

## Monitoring and Optimization

### Query History
- Review generated SQL in Genie interface
- Check SQL Warehouse query history
- Identify common patterns for view creation

### Performance
- Monitor warehouse utilization
- Create aggregated tables for common queries
- Use partitioning and Z-ordering on fact tables

### User Feedback
- Track which questions succeed/fail
- Refine instructions and comments based on usage
- Create FAQ for common questions

## Security and Governance

### Access Control
Genie respects all Unity Catalog security:
- Users only see data they have SELECT permission on
- Row-level security filters automatically applied
- Column masking honored in results

### Audit Logging
All Genie queries are logged:
- User who asked question
- Generated SQL
- Tables accessed
- Execution time
- Results returned

### Data Privacy
- Genie does not store query results
- LLM only sees metadata (not actual data)
- Sample data used for context is limited

## Advanced Features

### Custom Visualizations
After Genie generates a chart:
1. Click "Edit Visualization"
2. Customize chart type, axes, colors
3. Save for future use

### Scheduled Refreshes
Create dashboards from Genie queries:
1. Generate query in Genie
2. Click "Add to Dashboard"
3. Schedule automatic refresh

### Exporting Results
- Download as CSV
- Copy SQL to clipboard
- Share permalink to query
- Export to dashboard

## Troubleshooting

### Genie generates incorrect SQL
1. Check table/column comments - are they clear?
2. Review Genie Space instructions
3. Rephrase question with more specifics
4. Show Genie the correct SQL and ask it to learn

### Slow query performance
1. Check SQL - are there missing filters?
2. Look for missing is_current filters on SCD Type 2
3. Scale up SQL warehouse
4. Create indexed views for common patterns

### Missing data in results
1. Verify permissions (SELECT on all tables)
2. Check for row-level security filters
3. Verify data exists with direct SQL query
4. Look for date range filters excluding data

## Resources

### Documentation
- [Databricks Genie Official Docs](https://docs.databricks.com/en/genie/index.html)
- [Unity Catalog Comments Best Practices](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)
- [SQL Warehouse Configuration](https://docs.databricks.com/en/compute/sql-warehouse/index.html)

### Getting Help
- Check generated SQL first
- Review query history in SQL Warehouse
- Contact Databricks support for persistent issues
- Share feedback to improve Genie

## Conclusion

Databricks Genie democratizes data access by enabling natural language queries against your data warehouse. Success with Genie depends on:

1. **Well-documented data model** (comments, clear names)
2. **Proper Genie Space configuration** (instructions, data scope)
3. **Clear, specific questions** from users
4. **Iterative refinement** of queries and results

With proper setup and usage, Genie becomes a powerful tool for self-service analytics, reducing the burden on data teams while empowering business users to explore data independently.
