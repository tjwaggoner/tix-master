# Ticketmaster Data Pipeline Documentation

Welcome to the Ticketmaster data pipeline documentation. This directory contains comprehensive guides for understanding, deploying, and using the pipeline.

## Documentation Index

### Getting Started

#### [Setup & Deployment](setup/)
- **[SECRETS_SETUP.md](setup/SECRETS_SETUP.md)** - Configure Databricks secrets and API keys
- **[DEPLOYMENT.md](setup/DEPLOYMENT.md)** - Deploy the pipeline using Databricks Asset Bundles (DAB)

### Architecture & Design

#### [Architecture Documentation](architecture/)
- **[SURROGATE_KEYS.md](reference/SURROGATE_KEYS.md)** - Understanding surrogate key design patterns
- **[SCHEMA_RETENTION_ANALYSIS.md](architecture/SCHEMA_RETENTION_ANALYSIS.md)** - Data retention and schema evolution analysis
- **[PIPELINE_INFO.md](architecture/PIPELINE_INFO.md)** - Complete guide to medallion architecture implementation

### API Integration

#### [Ticketmaster API](api/)
- **[API_INFO.md](api/API_INFO.md)** - Ticketmaster Discovery API documentation, endpoints, and usage

### AI & Analytics

#### [Data Discovery with Genie](genie/)
- **[DATABRICKS_GENIE.md](genie/DATABRICKS_GENIE.md)** - Complete guide to Databricks Genie (AI-powered analytics)
- **[GENIE_INSTRUCTIONS.md](genie/GENIE_INSTRUCTIONS.md)** - Sample instructions for Genie configuration

#### Setup Guides
- **[SETUP_GENIE.md](setup/SETUP_GENIE.md)** - Quick setup guide for Genie Spaces

---

## Quick Navigation by Task

### I want to...

**Deploy the pipeline**
1. [Configure secrets](setup/SECRETS_SETUP.md)
2. [Deploy with DAB](setup/DEPLOYMENT.md)

**Understand the data model**
1. [Architecture overview](architecture/PIPELINE_INFO.md)
2. [Surrogate key patterns](reference/SURROGATE_KEYS.md)
3. [Schema retention](architecture/SCHEMA_RETENTION_ANALYSIS.md)

**Query data with natural language**
1. [Learn about Genie](genie/DATABRICKS_GENIE.md)
2. [Set up Genie Space](setup/SETUP_GENIE.md)
3. [Configure instructions](genie/GENIE_INSTRUCTIONS.md)

**Work with the API**
1. [API documentation](api/API_INFO.md)
2. [Data ingestion patterns](architecture/PIPELINE_INFO.md#ingestion-layer)

---

## Documentation Standards

### File Organization

```
docs/
├── README.md                      # This file - documentation index
├── DOCUMENTATION_STATUS.md        # Documentation tracking and status
├── api/                          # API integration documentation
│   └── API_INFO.md
├── architecture/                  # Architecture and design docs
│   ├── PIPELINE_INFO.md
│   └── SCHEMA_RETENTION_ANALYSIS.md
├── genie/                        # AI analytics with Genie
│   ├── DATABRICKS_GENIE.md
│   └── GENIE_INSTRUCTIONS.md
├── reference/                    # Reference documentation
│   └── SURROGATE_KEYS.md
└── setup/                        # Setup and deployment guides
    ├── DEPLOYMENT.md
    ├── SECRETS_SETUP.md
    └── SETUP_GENIE.md
```

### Naming Conventions
- **Uppercase with underscores**: All documentation files (SECRETS_SETUP.md, DATABRICKS_GENIE.md, PIPELINE_INFO.md)
- **Descriptive names**: Clear indication of content
- **Organized by topic**: Related docs grouped in subfolders

---

## Keeping Documentation Updated

### When to Update Documentation

**Setup/Deployment Changes**
- New secrets or configuration requirements → Update `setup/SECRETS_SETUP.md`
- DAB configuration changes → Update `setup/DEPLOYMENT.md`

**Architecture Changes**
- New tables or schema changes → Update `reference/SURROGATE_KEYS.md`
- Data retention policy changes → Update `architecture/SCHEMA_RETENTION_ANALYSIS.md`
- Pipeline design patterns → Update `architecture/PIPELINE_INFO.md`

**API Changes**
- New endpoints or parameters → Update `api/API_INFO.md`
- Rate limit changes → Update ingestion docs

**Feature Additions**
- New Genie capabilities → Update `genie/DATABRICKS_GENIE.md`
- Genie configuration → Update `genie/GENIE_INSTRUCTIONS.md`

---

## Contributing to Documentation

### Best Practices
1. **Keep it current** - Update docs when code changes
2. **Be specific** - Include examples and screenshots
3. **Cross-reference** - Link related documentation
4. **Test instructions** - Verify steps work before documenting
5. **Use consistent formatting** - Follow existing patterns

### Documentation Templates

**Setup Guide Template:**
```markdown
# Feature Setup Guide

## Prerequisites
- List requirements
- Check dependencies

## Configuration
1. Step-by-step instructions
2. Include code examples
3. Show expected output

## Verification
- How to test it worked
- Troubleshooting common issues

## Next Steps
- Link to related documentation
```

**Architecture Document Template:**
```markdown
# Feature Architecture

## Overview
High-level description

## Design Decisions
Why choices were made

## Implementation
Technical details

## Diagrams
Visual representations

## Trade-offs
Pros and cons of approach
```

---

## Documentation Status

| Document | Last Updated | Status |
|----------|-------------|--------|
| setup/SECRETS_SETUP.md | 2024-12-17 | Current |
| setup/DEPLOYMENT.md | 2024-12-17 | Current |
| setup/SETUP_GENIE.md | - | Needs review |
| api/API_INFO.md | 2025-01-06 | Current |
| architecture/PIPELINE_INFO.md | 2025-01-12 | Current |
| architecture/SCHEMA_RETENTION_ANALYSIS.md | 2025-01-07 | Current |
| genie/DATABRICKS_GENIE.md | 2025-01-12 | Current |
| genie/GENIE_INSTRUCTIONS.md | - | Needs review |
| reference/SURROGATE_KEYS.md | 2025-01-12 | Current |

---

## Questions or Feedback?

If you have questions about the documentation or suggestions for improvement:
1. Review existing docs first
2. Check the main [README.md](../README.md) for project overview
3. Consult the relevant section above
4. Reach out to the data engineering team

---

**Last Updated**: January 12, 2025
