# Databricks notebook source
"""
RAG Assistant for Event Q&A

This notebook implements a RAG (Retrieval Augmented Generation) system
for answering questions about Ticketmaster events using:
- Vector Search for semantic search
- Foundation Model API for embeddings
- LLM for response generation
"""

# COMMAND ----------

# MAGIC %md
# MAGIC # Ticketmaster Event RAG Assistant
# MAGIC
# MAGIC This notebook creates an AI assistant that can answer questions about events using:
# MAGIC 1. **Vector embeddings** of event descriptions
# MAGIC 2. **Semantic search** to find relevant events
# MAGIC 3. **LLM generation** to create natural language responses

# COMMAND ----------

# MAGIC %pip install databricks-vectorsearch mlflow

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient
from pyspark.sql.functions import concat_ws, col, coalesce, lit
import mlflow
import requests
import json

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = "ticket_master"
SCHEMA = "gold"
VECTOR_SEARCH_ENDPOINT = "ticket_master_vector_search"
INDEX_NAME = "events_index"

# Foundation Model endpoints
EMBEDDING_MODEL = "databricks-bge-large-en"
LLM_MODEL = "databricks-meta-llama-3-1-70b-instruct"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Prepare Event Documents

# COMMAND ----------

# Create a text representation of each event for embedding
event_documents = spark.sql(f"""
  SELECT
    f.event_id,
    f.event_sk,
    f.event_name,
    f.event_type,
    d.date_value as event_date,
    v.venue_name,
    v.city,
    v.state,
    v.country,
    a.attraction_name,
    a.genre_name,
    a.segment_name,
    f.price_min,
    f.price_max,
    f.price_currency,
    CONCAT_WS(' | ',
      CONCAT('Event: ', f.event_name),
      CONCAT('Type: ', f.event_type),
      CONCAT('Date: ', CAST(d.date_value AS STRING)),
      CONCAT('Venue: ', v.venue_name),
      CONCAT('Location: ', v.city, ', ', v.state, ', ', v.country),
      CONCAT('Attraction: ', COALESCE(a.attraction_name, 'N/A')),
      CONCAT('Genre: ', COALESCE(a.genre_name, 'N/A')),
      CONCAT('Price Range: ', CAST(f.price_min AS STRING), '-', CAST(f.price_max AS STRING), ' ', f.price_currency)
    ) as event_text
  FROM {CATALOG}.{SCHEMA}.fact_events f
  INNER JOIN {CATALOG}.{SCHEMA}.dim_date d ON f.event_date_key = d.date_key
  INNER JOIN {CATALOG}.{SCHEMA}.dim_venue v ON f.venue_sk = v.venue_sk
  LEFT JOIN {CATALOG}.{SCHEMA}.dim_attraction a ON f.attraction_sk = a.attraction_sk
  WHERE f.is_test = FALSE
    AND d.date_value >= CURRENT_DATE()
""")

display(event_documents.limit(5))

# COMMAND ----------

# Save event documents to a table
event_documents.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.event_documents")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Vector Search Index

# COMMAND ----------

# Initialize Vector Search client
vsc = VectorSearchClient()

# COMMAND ----------

# Create endpoint if it doesn't exist
try:
    vsc.create_endpoint(
        name=VECTOR_SEARCH_ENDPOINT,
        endpoint_type="STANDARD"
    )
    print(f"Created endpoint: {VECTOR_SEARCH_ENDPOINT}")
except Exception as e:
    print(f"Endpoint may already exist: {e}")

# COMMAND ----------

# Create vector search index
try:
    index = vsc.create_delta_sync_index(
        endpoint_name=VECTOR_SEARCH_ENDPOINT,
        source_table_name=f"{CATALOG}.{SCHEMA}.event_documents",
        index_name=f"{CATALOG}.{SCHEMA}.{INDEX_NAME}",
        pipeline_type="TRIGGERED",
        primary_key="event_id",
        embedding_source_column="event_text",
        embedding_model_endpoint_name=EMBEDDING_MODEL
    )
    print(f"Created index: {INDEX_NAME}")
except Exception as e:
    print(f"Index creation error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Search Functions

# COMMAND ----------

def semantic_search(query: str, num_results: int = 5):
    """
    Perform semantic search on event documents
    
    Args:
        query: Natural language query
        num_results: Number of results to return
    
    Returns:
        List of relevant event documents
    """
    index = vsc.get_index(
        endpoint_name=VECTOR_SEARCH_ENDPOINT,
        index_name=f"{CATALOG}.{SCHEMA}.{INDEX_NAME}"
    )
    
    results = index.similarity_search(
        query_text=query,
        columns=["event_id", "event_name", "event_date", "venue_name", "city", "state", "event_text"],
        num_results=num_results
    )
    
    return results

# COMMAND ----------

# Test semantic search
test_query = "concerts in New York next month"
search_results = semantic_search(test_query, num_results=3)
print(f"Query: {test_query}\n")
print(search_results)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: LLM Response Generation

# COMMAND ----------

def generate_response(query: str, context_docs: list) -> str:
    """
    Generate a natural language response using LLM
    
    Args:
        query: User's question
        context_docs: Retrieved relevant documents
    
    Returns:
        Generated response
    """
    # Format context
    context = "\n\n".join([
        f"Event {i+1}: {doc['event_text']}"
        for i, doc in enumerate(context_docs)
    ])
    
    # Create prompt
    prompt = f"""You are a helpful assistant for Ticketmaster event information.

Context (Relevant Events):
{context}

User Question: {query}

Based on the events provided above, answer the user's question in a helpful and conversational way.
If the question cannot be answered from the context, say so.
Include specific event details like names, dates, venues, and locations.

Answer:"""
    
    # Call LLM via Foundation Model API
    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
        
        w = WorkspaceClient()
        
        response = w.serving_endpoints.query(
            name=LLM_MODEL,
            messages=[
                ChatMessage(role=ChatMessageRole.SYSTEM, content="You are a helpful Ticketmaster event assistant."),
                ChatMessage(role=ChatMessageRole.USER, content=prompt)
            ],
            max_tokens=500,
            temperature=0.7
        )
        
        return response.choices[0].message.content
    except Exception as e:
        return f"Error calling LLM: {str(e)}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Complete RAG Pipeline

# COMMAND ----------

def ask_event_assistant(question: str, num_results: int = 5) -> dict:
    """
    Complete RAG pipeline: search + generate
    
    Args:
        question: User's question
        num_results: Number of documents to retrieve
    
    Returns:
        Dictionary with question, retrieved docs, and answer
    """
    print(f"Question: {question}")
    print("\n🔍 Searching for relevant events...")
    
    # Step 1: Semantic search
    search_results = semantic_search(question, num_results)
    
    print(f"✓ Found {len(search_results.get('result', {}).get('data_array', []))} relevant events")
    print("\n🤖 Generating response...")
    
    # Step 2: Generate response
    docs = search_results.get('result', {}).get('data_array', [])
    answer = generate_response(question, docs)
    
    print("\n✓ Response generated!\n")
    print(f"Answer: {answer}")
    
    return {
        "question": question,
        "retrieved_events": docs,
        "answer": answer
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Test the Assistant

# COMMAND ----------

# Test query 1
result1 = ask_event_assistant("What concerts are happening in Los Angeles next weekend?")

# COMMAND ----------

# Test query 2
result2 = ask_event_assistant("Are there any sports events in New York in December?")

# COMMAND ----------

# Test query 3
result3 = ask_event_assistant("Show me rock concerts with tickets under $100")

# COMMAND ----------

# Test query 4
result4 = ask_event_assistant("What are the most popular venues for music events?")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Create Interactive Widget

# COMMAND ----------

# Create interactive widget for questions
dbutils.widgets.text("user_question", "", "Ask about events:")

# COMMAND ----------

# Get question from widget and respond
user_question = dbutils.widgets.get("user_question")

if user_question:
    result = ask_event_assistant(user_question)
    displayHTML(f"""
    <div style="padding: 20px; background: #f0f0f0; border-radius: 10px; margin: 10px;">
        <h3>Question:</h3>
        <p>{result['question']}</p>
        <h3>Answer:</h3>
        <p>{result['answer']}</p>
    </div>
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Maintenance

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Refresh vector index when new events are added
# MAGIC -- This can be automated with a job
# MAGIC ALTER INDEX ticket_master.gold.events_index SYNC;

# COMMAND ----------

# Monitor index status
index_status = vsc.get_index(
    endpoint_name=VECTOR_SEARCH_ENDPOINT,
    index_name=f"{CATALOG}.{SCHEMA}.{INDEX_NAME}"
)
print(index_status)
