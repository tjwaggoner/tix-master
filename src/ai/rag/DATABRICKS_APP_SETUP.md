# Databricks App Setup Instructions

## 📱 Setting Up Your Events AI RAG App

You've created a Databricks App called `events-ai-rag`. Here's how to configure it:

### Step 1: Configure the App

1. Go to **Compute** → **Apps** in Databricks
2. Click on your app: `events-ai-rag`
3. Click **Configure** or **Edit Settings**

### Step 2: Set the Source File

Configure the app to use the RAG application code:

**Option A: If using deployed files**
- **Source Type**: Workspace file
- **Path**: `/Workspace/Users/tanner.waggoner@databricks.com/.bundle/tix-master/dev/files/src/ai/rag/app.py`

**Option B: If using Git sync (recommended)**
- **Source Type**: Git repository
- **Repository**: `https://github.com/tjwaggoner/tix-master.git`
- **Branch**: `main`
- **Path in repo**: `src/ai/rag/app.py`

### Step 3: Configure Dependencies

Set the Python dependencies file:

- **Requirements file**: `src/ai/rag/requirements.txt`

Or manually add:
```
gradio>=4.0.0
databricks-sdk>=0.18.0
```

### Step 4: Set Environment Variables (if needed)

If you need to customize the endpoint name:

- **Name**: `ENDPOINT_NAME`
- **Value**: `event-rag-assistant`

### Step 5: Configure Compute

- **Compute size**: Small (or as needed)
- **Scale to zero**: Enabled (to save costs)

### Step 6: Start the App

1. Click **Start** or **Deploy**
2. Wait for app to be in "Running" state (~2-3 minutes)
3. Click on the **URL** to open your app
4. Share the URL with your team!

## 🔍 Troubleshooting

### App won't start?

**Check Prerequisites:**
1. ✅ ETL pipeline has run (data exists in `ticket_master.gold`)
2. ✅ Vector search setup is complete (`setup_vector_search.py`)
3. ✅ Serving endpoint is deployed and ready (`deploy_serving_endpoint.py`)

**Check Logs:**
- Click on the app name
- Go to **Logs** tab
- Look for Python errors or missing dependencies

### "Endpoint not found" error?

Make sure the serving endpoint `event-rag-assistant` is deployed:

```python
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
endpoint = w.serving_endpoints.get("event-rag-assistant")
print(f"Status: {endpoint.state.ready}")
```

If not ready, run `src/ai/rag/deploy_serving_endpoint.py`

### Dependencies not installing?

Make sure `requirements.txt` is in the same folder as `app.py`:
- `src/ai/rag/app.py`
- `src/ai/rag/requirements.txt`

## 🎉 Success!

Once running, your app will be accessible at:
`https://<workspace-url>/serving-endpoints/events-ai-rag/`

Share this URL with your team for a permanent, always-on RAG assistant! 🚀

