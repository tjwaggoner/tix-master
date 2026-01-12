# Databricks Asset Bundle (DAB) Deployment Guide

This guide covers deploying and managing the Tix Master pipeline using Databricks Asset Bundles (DAB) from your IDE.

## Prerequisites

1. **Install Databricks CLI**
   ```bash
   pip install databricks-cli
   ```

2. **Authenticate with Databricks**
   ```bash
   databricks configure --token
   ```
   
   When prompted:
   - Host: `https://fe-vm-tw-vdm-serverless-tixsfe.cloud.databricks.com`
   - Token: Your personal access token (create one in Databricks UI: Settings > User Settings > Access Tokens)

3. **Set Up Databricks Secrets**
   Configure your Ticketmaster API key:
   ```bash
   # Create secret scope
   databricks secrets create-scope tix-master
   
   # Add API key (opens editor to paste your key)
   databricks secrets put-secret tix-master ticketmaster-api-key
   ```
   
   Get your API key from: https://developer.ticketmaster.com/
   
   See `SECRETS_SETUP.md` for detailed instructions.

## Quick Start - Deploy from IDE

### 1. Validate Your Bundle
Before deploying, validate the configuration:
```bash
databricks bundle validate
```

### 2. Deploy to Development
Deploy to your personal dev workspace (default target):
```bash
databricks bundle deploy
```

This will:
- Upload all source files to your workspace
- Create/update job definitions
- Use your personal schemas with your username prefix
- Deploy to: `~/.bundle/tix-master/dev`

### 3. Run Your Pipeline
After deployment, trigger the ETL pipeline:
```bash
# Run the main ETL pipeline (includes automatic setup on first run)
databricks bundle run tix_master_etl_pipeline
```

### 4. Monitor Job Runs
View job status:
```bash
databricks bundle run tix_master_etl_pipeline --watch
```

Or view in Databricks UI:
- Navigate to **Workflows** in your workspace
- Find `[dev] Tix Master ETL Pipeline`
- Click to see run history and logs

## Deployment Targets

### Development Target (default)
```bash
databricks bundle deploy
# or explicitly:
databricks bundle deploy --target dev
```

**Characteristics:**
- Mode: Development (allows live editing)
- Personal schemas: `{username}_bronze`, `{username}_silver`, `{username}_gold`
- Deployed to: `~/.bundle/tix-master/dev`
- Uses serverless compute
- Email notifications to you

### Staging Target
```bash
databricks bundle deploy --target staging
```

**Characteristics:**
- Mode: Production
- Shared schemas: `staging_bronze`, `staging_silver`, `staging_gold`
- Deployed to: `~/.bundle/tix-master/staging`
- Runs as service principal
- For testing before production

### Production Target
```bash
databricks bundle deploy --target prod
```

**Characteristics:**
- Mode: Production
- Production schemas: `bronze`, `silver`, `gold`
- Deployed to: `~/.bundle/tix-master/prod`
- Runs as service principal
- Requires admin permissions

## Common Workflows

### Making Changes and Redeploying

1. **Edit your code** in the IDE (e.g., `src/bronze/bronze_auto_loader.py`)

2. **Deploy the changes:**
   ```bash
   databricks bundle deploy
   ```

3. **Run the updated job:**
   ```bash
   databricks bundle run tix_master_etl_pipeline
   ```

### Quick Iteration in Dev Mode

In development mode, you can sync your local changes automatically:
```bash
# Watch for changes and auto-sync
databricks sync
```

This keeps your workspace files in sync with local changes without full redeployment.

### Running Individual Tasks

To run a specific task instead of the full pipeline:
```bash
# Just refresh bronze layer
databricks bundle run tix_master_bronze_refresh
```

### View Job Configuration

See what will be deployed:
```bash
databricks bundle deploy --dry-run
```

See the fully interpolated configuration:
```bash
databricks bundle deploy --target dev --dry-run --verbose
```

## Troubleshooting

### Authentication Issues
```bash
# Re-configure your credentials
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
1. Check logs in Databricks UI (Workflows > Job > Run Details)
2. Verify environment variables are set correctly
3. Check Unity Catalog permissions
4. Verify cluster configuration

### Schema/Permissions Issues
Ensure you have:
- `USE CATALOG` on `ticket_master`
- `CREATE SCHEMA` on the catalog
- `USE SCHEMA` on all schemas
- `READ FILES` on volumes

## IDE Integration

### VS Code / Cursor
1. Open terminal in IDE
2. Navigate to project root: `cd ~/Documents/tix-master`
3. Run deployment commands directly from terminal
4. Use "Run Task" feature to create shortcuts:
   - Add to `.vscode/tasks.json`:
   ```json
   {
     "version": "2.0.0",
     "tasks": [
       {
         "label": "Deploy DAB to Dev",
         "type": "shell",
         "command": "databricks bundle deploy",
         "problemMatcher": []
       },
       {
         "label": "Run ETL Pipeline",
         "type": "shell",
         "command": "databricks bundle run tix_master_etl_pipeline",
         "problemMatcher": []
       }
     ]
   }
   ```

### Keyboard Shortcuts
You can set up custom keyboard shortcuts for common operations:
- Deploy: `Cmd+Shift+D`
- Run Pipeline: `Cmd+Shift+R`

## File Structure

```
tix-master/
├── databricks.yml              # Main DAB configuration
├── resources/
│   └── jobs.yml                # Job definitions
├── src/                        # Python source code (auto-uploaded)
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── ingestion/
├── sql/                        # SQL scripts (auto-uploaded)
│   ├── ddl/
│   └── stored_procedures/
├── config/
│   └── config.yaml             # Application config
└── .databricks/                # Local deployment state (ignored by git)
```

## Configuration Variables

Edit `databricks.yml` to customize:
- `catalog_name`: Unity Catalog name
- `bronze_schema`, `silver_schema`, `gold_schema`: Schema names per target
- `volume_path`: Location of raw data files
- `workspace.host`: Databricks workspace URL
- `compute_id`: Cluster or "serverless"

## Security Best Practices

1. **Never commit credentials** to git
2. **Use personal access tokens** for dev
3. **Use service principals** for staging/prod
4. **Store secrets** in Databricks Secrets:
   ```bash
   databricks secrets create-scope --scope tix-master
   databricks secrets put --scope tix-master --key ticketmaster-api-key
   ```

## Next Steps

1. **Deploy and run:**
   ```bash
   databricks bundle deploy
   databricks bundle run tix_master_etl_pipeline
   ```
   
   The pipeline automatically handles setup on the first run!

2. **View results in Databricks:**
   - Check Unity Catalog for your tables
   - Query data in SQL Editor
   - View job runs in Workflows

## Additional Resources

- [Databricks Asset Bundles Documentation](https://docs.databricks.com/dev-tools/bundles/)
- [Databricks CLI Reference](https://docs.databricks.com/dev-tools/cli/index.html)
- [Unity Catalog Best Practices](https://docs.databricks.com/data-governance/unity-catalog/best-practices.html)

