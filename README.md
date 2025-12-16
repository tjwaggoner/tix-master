# Ticketmaster Medallion Architecture

A comprehensive data lakehouse implementation for Ticketmaster API data using Databricks Unity Catalog with Bronze/Silver/Gold layers.

## Architecture Overview

This project implements a Medallion Architecture for Ticketmaster public endpoints:

- **Bronze Layer**: Raw JSON data ingestion using Auto Loader with deduplication
- **Silver Layer**: Normalized relational tables with PK/FK constraints and proper ERD
- **Gold Layer**: Star schemas with identity keys, materialized views for BI consumption

## Features

- Unity Catalog Volumes for staging raw JSON
- Auto Loader streaming from UC Volumes to Bronze Delta tables
- Rigorous ERD with PK/FK constraints in Silver layer
- Star schema design in Gold layer with identity surrogate keys
- SQL Warehouse stored procedures with control flow
- AI/BI Genie integration for natural language queries
- RAG assistant for event Q&A
- Lakeflow Jobs orchestration
- CI/CD with Databricks Asset Bundles

## Project Structure

```
tix-master/
├── src/
│   ├── ingestion/          # API ingestion scripts
│   ├── bronze/             # Bronze layer notebooks
│   ├── silver/             # Silver layer transformation notebooks
│   ├── gold/               # Gold layer star schema notebooks
│   └── ai/                 # AI/RAG assistant notebooks
├── sql/
│   ├── ddl/                # Table definitions
│   ├── stored_procedures/  # SQL Warehouse procedures
│   └── queries/            # Sample queries
├── config/                 # Configuration files
├── databricks-asset-bundles/ # Asset bundle definitions
├── tests/                  # Unit and integration tests
└── docs/                   # Additional documentation
```

## Resources

- [Ticketmaster API Documentation](https://developer.ticketmaster.com/products-and-docs/apis/getting-started/)
- [Databricks PK/FK Constraints](https://www.databricks.com/blog/primary-key-and-foreign-key-constraints-are-ga-and-now-enable-faster-queries)
- [Data Warehousing Demo](https://www.databricks.com/resources/demos/tutorials/data-warehouse/data-warehousing-with-identity-primary-key-and-foreign-key)

## Getting Started

1. Configure Databricks workspace connection
2. Set Ticketmaster API key in config
3. Run ingestion pipeline
4. Execute Bronze/Silver/Gold transformations
5. Enable AI/BI Genie

## Unity Catalog Structure

```
catalog: ticketmaster
├── schema: bronze
├── schema: silver
├── schema: gold
└── volumes: raw_data
```
