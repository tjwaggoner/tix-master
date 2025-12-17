# Secrets Setup Guide

This guide shows you how to configure your Ticketmaster API key in Databricks secrets.

## Prerequisites

1. Get a Ticketmaster API key from: https://developer.ticketmaster.com/
   - Sign up for a free account
   - Create an app to get your API key

2. Have Databricks CLI installed and authenticated (you already did this!)

## Setup Steps

### Step 1: Create a Secret Scope

```bash
databricks secrets create-scope tix-master
```

This creates a secret scope named `tix-master` to store your sensitive credentials.

### Step 2: Add Your API Key

```bash
databricks secrets put-secret tix-master ticketmaster-api-key
```

This will open a text editor where you can paste your Ticketmaster API key. 
- Paste your API key
- Save and close the editor

**Note:** The format is `databricks secrets put-secret <scope> <key-name>`

### Step 3: Verify (Optional)

```bash
# List all secrets in the scope
databricks secrets list-secrets tix-master
```

You should see `ticketmaster-api-key` listed (but not the actual value - it's secret!)

## Alternative: Using Databricks UI

You can also set up secrets through the Databricks UI:

1. Go to your workspace: https://fe-vm-tw-vdm-serverless-tixsfe.cloud.databricks.com
2. Click on your username → Settings → Developer
3. Go to "Secrets" section
4. Create scope: `tix-master`
5. Add secret: `ticketmaster-api-key` with your API key value

## Testing Your Setup

After setting up the secret, run the pipeline:

```bash
databricks bundle run tix_master_etl_pipeline
```

The ingestion notebook will automatically retrieve the API key from secrets and fetch data from Ticketmaster.

## Security Best Practices

✅ **DO:**
- Use secrets for all API keys and passwords
- Use different scopes for dev/staging/prod
- Limit access to secret scopes

❌ **DON'T:**
- Hardcode API keys in notebooks or code
- Share API keys in chat or email
- Commit secrets to git

## Troubleshooting

### "Secret scope not found"
Run: `databricks secrets create-scope tix-master`

### "Secret not found"
Run: `databricks secrets put-secret tix-master ticketmaster-api-key`

### "Permission denied"
Ask your Databricks admin to grant you access to create/read secrets.

