# RAG Event Assistant

AI-powered Q&A system for Ticketmaster events using Retrieval Augmented Generation.

## 📁 Files

- **`setup_vector_search.py`** - Creates vector search endpoint and index, includes interactive query widget
- **`sync_vector_index.py`** - Auto-sync vector embeddings (runs in pipeline after ETL)

## 🚀 Quick Start

### Step 1: Setup Vector Search (One-time)

1. Open `setup_vector_search.py` in Databricks
2. Run all cells to create:
   - Vector search endpoint: `ticket_master_vector_search`
   - Vector index: `ticket_master.gold.events_index`
   - Event documents table for RAG

### Step 2: Query Events

Use the interactive widget in `setup_vector_search.py`:
- Enter questions in natural language
- Get AI-generated answers with event details
- No code required!

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
RAG Assistant (setup_vector_search.py)
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

## 🔧 Troubleshooting

**No results from queries?**
- Ensure ETL pipeline has run
- Check `ticket_master.gold.event_documents` has data
- Verify vector index exists and is synced

**Vector index not syncing?**
- Check pipeline task 8 (`sync_vector_index`) completed successfully
- Verify Change Data Feed is enabled on `event_documents`
- Run sync manually in `setup_vector_search.py`

