# RAG Event Assistant

AI-powered Q&A system for Ticketmaster events using Retrieval Augmented Generation.

## 📁 Files

- **`rag_assistant.py`** - Full RAG notebook (setup + testing)
- **`rag_model.py`** - MLflow model + serving endpoint deployment
- **`webapp.py`** - Gradio web application
- **`sync_vector_index.py`** - Auto-sync vector embeddings (runs in pipeline)

## 🚀 Quick Start

### Step 1: Deploy Model Serving Endpoint

1. Open `rag_model.py` in Databricks
2. Run all cells to:
   - Test the RAG model locally
   - Register model to Unity Catalog
   - Deploy as serving endpoint: `event-rag-assistant`

**Wait for endpoint to be ready** (~5-10 minutes)

### Step 2: Launch Web App

1. Open `webapp.py` in Databricks
2. Run all cells
3. A shareable URL will appear - open it in your browser
4. Ask questions about events!

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

