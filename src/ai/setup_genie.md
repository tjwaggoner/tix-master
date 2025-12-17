# AI/BI Genie Setup Guide

This guide explains how to enable and configure AI/BI Genie for natural language queries on the Ticketmaster data lakehouse.

## What is AI/BI Genie?

AI/BI Genie is Databricks' natural language interface for SQL Warehouses. It allows users to ask questions in plain English and automatically generates SQL queries to answer them.

## Prerequisites

- SQL Warehouse (Serverless recommended)
- Unity Catalog enabled
- Gold layer tables created and optimized
- Appropriate permissions on the catalog

## Step 1: Create a Genie Space

1. Navigate to your Databricks workspace
2. Click on "AI/BI" in the left sidebar
3. Click "Create Genie Space"
4. Configure the space:
   - **Name**: Ticketmaster Events Analytics
   - **Description**: Natural language interface for querying Ticketmaster events, venues, and attractions
   - **SQL Warehouse**: Select your warehouse
   - **Catalog**: `ticket_master`
   - **Schema**: `gold`

## Step 2: Configure Instructions

Add these instructions to guide Genie's behavior:

```
You are an expert data analyst for Ticketmaster event data. Help users understand:
- Event trends and patterns
- Venue popularity and capacity
- Attraction/artist performance
- Geographic distribution of events
- Pricing analysis
- Seasonal patterns

Key tables:
- fact_events: Main event facts with dates, prices, and status
- dim_venue: Venue information including location
- dim_attraction: Artists, teams, and performers
- dim_date: Date dimension with calendar attributes
- dim_classification: Event classifications (genre, segment)
- dim_market: Geographic markets

When generating queries:
- Always filter out test events (is_test = FALSE)
- Use the star schema with proper joins via surrogate keys
- Prefer materialized views (mv_*) for aggregations when available
```

## Step 3: Add Sample Questions

Configure these example questions to help users:

1. "How many events are scheduled next month?"
2. "What are the top 10 venues by number of events?"
3. "Show me average ticket prices by genre"
4. "Which cities have the most concerts?"
5. "What's the trend of events over time by month?"
6. "Compare weekend vs weekday events"
7. "Show me the most popular attractions in California"
8. "What percentage of events are music vs sports?"

## Step 4: Grant Access

```sql
-- Grant read access to relevant users/groups
GRANT SELECT ON CATALOG ticket_master TO `data_analysts`;
GRANT SELECT ON SCHEMA ticket_master.gold TO `data_analysts`;
GRANT SELECT ON ALL TABLES IN SCHEMA ticket_master.gold TO `data_analysts`;

-- Grant Genie usage
GRANT USE GENIE ON SPACE `Ticketmaster Events Analytics` TO `data_analysts`;
```

## Step 5: Test the Genie Space

Try these test queries:

1. **Simple query**: "How many events do we have?"
2. **Aggregation**: "Show me the top 5 venues by number of events"
3. **Time-based**: "What is the distribution of events by month?"
4. **Join query**: "List events with their venue cities and attraction names"
5. **Complex analysis**: "Compare average ticket prices between music and sports events"

## Step 6: Monitor and Refine

1. Review the SQL queries Genie generates
2. Add synonyms for domain-specific terms:
   - "concerts" → events where segment_name = 'Music'
   - "sports games" → events where segment_name = 'Sports'
   - "shows" → events
   - "performers" → attractions

3. Create saved queries for common patterns
4. Update instructions based on user feedback

## Tips for Best Results

1. **Optimize tables**: Ensure all Gold tables have statistics
   ```sql
   ANALYZE TABLE ticket_master.gold.fact_events COMPUTE STATISTICS FOR ALL COLUMNS;
   ```

2. **Use materialized views**: Create views for common queries
3. **Add table/column comments**: Help Genie understand the schema
   ```sql
   COMMENT ON TABLE ticket_master.gold.fact_events IS 'Core event facts';
   COMMENT ON COLUMN ticket_master.gold.fact_events.price_max IS 'Maximum ticket price';
   ```

4. **Set up row-level security**: If needed, restrict data access by user

## Troubleshooting

### Genie generates incorrect SQL
- Check if table statistics are up to date
- Verify instructions are clear and specific
- Add more sample questions covering similar patterns

### Performance issues
- Optimize tables with Z-ORDER
- Create materialized views for expensive queries
- Consider caching frequently accessed data

### Access denied errors
- Verify Unity Catalog permissions
- Check SQL Warehouse access
- Ensure Genie Space permissions are granted

## Next Steps

- Set up dashboards using Genie-generated queries
- Create scheduled reports
- Integrate with Slack/Teams for notifications
- Build custom visualizations in Databricks SQL
