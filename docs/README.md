# Ticketmaster Data Pipeline Documentation

Welcome to the Ticketmaster data pipeline documentation. This directory contains comprehensive guides for understanding, deploying, and using the pipeline.

## 📚 Documentation Index

### 🚀 Getting Started

#### [Setup & Deployment](setup/)
- **[SECRETS_SETUP.md](setup/SECRETS_SETUP.md)** - Configure Databricks secrets and API keys
- **[DEPLOYMENT.md](setup/DEPLOYMENT.md)** - Deploy the pipeline using Databricks Asset Bundles (DAB)

### 🏗️ Architecture & Design

#### [Architecture Documentation](architecture/)
- **[SURROGATE_KEYS.md](SURROGATE_KEYS.md)** - Understanding surrogate key design patterns
- **[SCHEMA_RETENTION_ANALYSIS.md](architecture/SCHEMA_RETENTION_ANALYSIS.md)** - Data retention and schema evolution analysis
- **[databricks-pipeline-ebook.md](architecture/databricks-pipeline-ebook.md)** - Complete guide to medallion architecture implementation

### 🔌 API Integration

#### [Ticketmaster API](api/)
- **[API_INFO.md](api/API_INFO.md)** - Ticketmaster Discovery API documentation, endpoints, and usage

### 🤖 AI & Analytics

#### Data Discovery
- **[databricks-genie.md](databricks-genie.md)** - Complete guide to Databricks Genie (AI-powered analytics)
- **[setup_genie.md](setup_genie.md)** - Quick setup guide for Genie Spaces
- **[genie_instructions.md](genie_instructions.md)** - Sample instructions for Genie configuration

#### Dashboards & Visualization
- **[lakeview-dashboard.md](lakeview-dashboard.md)** - Lakeview dashboard creation and management

---

## 🎯 Quick Navigation by Task

### I want to...

**Deploy the pipeline**
1. [Configure secrets](setup/SECRETS_SETUP.md)
2. [Deploy with DAB](setup/DEPLOYMENT.md)

**Understand the data model**
1. [Architecture overview](architecture/databricks-pipeline-ebook.md)
2. [Surrogate key patterns](SURROGATE_KEYS.md)
3. [Schema retention](architecture/SCHEMA_RETENTION_ANALYSIS.md)

**Query data with natural language**
1. [Learn about Genie](databricks-genie.md)
2. [Set up Genie Space](setup_genie.md)
3. [Configure instructions](genie_instructions.md)

**Work with the API**
1. [API documentation](api/API_INFO.md)
2. [Data ingestion patterns](architecture/databricks-pipeline-ebook.md#ingestion-layer)

**Create dashboards**
1. [Lakeview dashboard guide](lakeview-dashboard.md)

---

## 📖 Documentation Standards

### File Organization

```
docs/
├── README.md                      # This file - documentation index
├── setup/                         # Setup and deployment guides
│   ├── SECRETS_SETUP.md
│   └── DEPLOYMENT.md
├── architecture/                  # Architecture and design docs
│   ├── SCHEMA_RETENTION_ANALYSIS.md
│   └── databricks-pipeline-ebook.md
├── api/                          # API integration documentation
│   └── API_INFO.md
├── SURROGATE_KEYS.md             # Data modeling patterns
├── databricks-genie.md           # Genie comprehensive guide
├── setup_genie.md                # Genie quick setup
├── genie_instructions.md         # Genie configuration
└── lakeview-dashboard.md         # Dashboard guide
```

### Naming Conventions
- **Uppercase**: Major guides (SECRETS_SETUP.md, DEPLOYMENT.md)
- **Lowercase hyphenated**: Feature-specific docs (databricks-genie.md, lakeview-dashboard.md)
- **Descriptive names**: Clear indication of content

---

## 🔄 Keeping Documentation Updated

### When to Update Documentation

**Setup/Deployment Changes**
- New secrets or configuration requirements → Update `setup/SECRETS_SETUP.md`
- DAB configuration changes → Update `setup/DEPLOYMENT.md`

**Architecture Changes**
- New tables or schema changes → Update `SURROGATE_KEYS.md`
- Data retention policy changes → Update `architecture/SCHEMA_RETENTION_ANALYSIS.md`

**API Changes**
- New endpoints or parameters → Update `api/API_INFO.md`
- Rate limit changes → Update ingestion docs

**Feature Additions**
- New Genie capabilities → Update `databricks-genie.md`
- New dashboard templates → Update `lakeview-dashboard.md`

---

## 💡 Contributing to Documentation

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

## 🔍 Documentation Status

| Document | Last Updated | Status |
|----------|-------------|--------|
| SECRETS_SETUP.md | 2024-12-17 | ✅ Current |
| DEPLOYMENT.md | 2024-12-17 | ✅ Current |
| API_INFO.md | 2025-01-06 | ✅ Current |
| SURROGATE_KEYS.md | 2025-01-12 | ✅ Current |
| databricks-genie.md | 2025-01-12 | ✅ Current |
| SCHEMA_RETENTION_ANALYSIS.md | 2025-01-07 | ✅ Current |
| databricks-pipeline-ebook.md | 2025-01-06 | ✅ Current |
| setup_genie.md | - | ⚠️ Needs review |
| genie_instructions.md | - | ⚠️ Needs review |
| lakeview-dashboard.md | - | ⚠️ Needs review |

---

## 📧 Questions or Feedback?

If you have questions about the documentation or suggestions for improvement:
1. Review existing docs first
2. Check the main [README.md](../README.md) for project overview
3. Consult the relevant section above
4. Reach out to the data engineering team

---

**Last Updated**: January 12, 2025
