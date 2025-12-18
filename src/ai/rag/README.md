# RAG Event Assistant

AI-powered Q&A system for Ticketmaster events using Retrieval Augmented Generation.

## 📁 Files

- **`setup_vector_search.py`** - Creates vector search endpoint and index (one-time setup)
- **`deploy_serving_endpoint.py`** - Creates MLflow model and serving endpoint for API access
- **`app.py`** - Gradio web app for Databricks Apps (permanent deployment)
- **`webapp.py`** - Gradio notebook for ad-hoc testing (temporary URLs)
- **`sync_vector_index.py`** - Auto-sync vector embeddings (runs in pipeline after ETL)
- **`requirements.txt`** - Python dependencies for Databricks Apps
- **`DATABRICKS_APP_SETUP.md`** - Step-by-step app configuration guide

## 🚀 Quick Start

### Step 1: Setup Vector Search (One-time)

1. Open `setup_vector_search.py` in Databricks
2. Run all cells to create:
   - Vector search endpoint: `ticket_master_vector_search`
   - Vector index: `ticket_master.gold.events_index`

### Step 2: Deploy Model Serving Endpoint

1. Open `deploy_serving_endpoint.py` in Databricks
2. Run all cells to:
   - Test the RAG model locally
   - Register model to Unity Catalog
   - Deploy as serving endpoint: `event-rag-assistant`

**Wait for endpoint to be ready** (~5-10 minutes)

### Step 3: Deploy Web App

**Option A: Databricks Apps (Recommended - Permanent)**

1. Go to **Compute** → **Apps** in Databricks
2. Create new app or use existing: `events-ai-rag`
3. Set source file: `src/ai/rag/app.py`
4. Set requirements: `src/ai/rag/requirements.txt`
5. Start the app
6. Get permanent URL to share with team

See `DATABRICKS_APP_SETUP.md` for detailed configuration steps.

**Option B: Notebook (Quick Testing - Temporary)**

1. Open `webapp.py` in Databricks
2. Run all cells
3. A temporary shareable URL will appear
4. URL expires when notebook is stopped

## 💬 Example Questions

- "What concerts are happening in Los Angeles this weekend?"
- "Show me sports events in New York"
- "Are there any rock concerts under $100?"
- "What venues host the most events?"
- "Find family-friendly events in California"

## 🏗️ Architecture

```
User Question
    ↓
Web App (Gradio)
    ↓
Model Serving Endpoint
    ↓
RAG Model (MLflow)
    ├─→ Vector Search (find similar events)
    └─→ LLM (Llama 3.1 70B) - generate answer
    ↓
Answer with event details
```

## 🔄 Data Sync

The vector index automatically syncs with new events via:
- Pipeline Task 8: `sync_vector_index.py`
- Runs after each ETL job
- Keeps embeddings fresh

## 🛠️ Components

**Vector Search:**
- Endpoint: `ticket_master_vector_search`
- Index: `ticket_master.gold.events_index`
- Embedding Model: `databricks-bge-large-en`

**LLM:**
- Model: `databricks-meta-llama-3-1-70b-instruct`
- Temperature: 0.7 (conversational)
- Max Tokens: 500

**Model Serving:**
- Endpoint: `event-rag-assistant`
- Workload: Small with scale-to-zero
- Location: Unity Catalog (`ticket_master.gold.event_rag_model`)

## 📊 Monitoring

Check endpoint status:
```python
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
status = w.serving_endpoints.get("event-rag-assistant")
print(status.state)
```

## 🔧 Troubleshooting

**Endpoint not ready?**
- Check Serving Endpoints UI for status
- Wait for "Ready" state
- Check logs if failed

**No results?**
- Ensure ETL pipeline has run
- Check `ticket_master.gold.event_documents` has data
- Verify vector index exists

**Web app not loading?**
- Make sure endpoint is deployed and ready
- Check endpoint name matches in `webapp.py`
- Try restarting the Gradio cell

