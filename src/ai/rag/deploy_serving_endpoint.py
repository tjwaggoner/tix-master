# Databricks notebook source
"""
MLflow Model for RAG Event Assistant
Packages the RAG system as a deployable model serving endpoint
"""

# COMMAND ----------

# MAGIC %pip install databricks-vectorsearch mlflow --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import mlflow
import pandas as pd
from databricks.vector_search.client import VectorSearchClient
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

# COMMAND ----------

# Configuration
CATALOG = "ticket_master"
SCHEMA = "gold"
VECTOR_SEARCH_ENDPOINT = "ticket_master_vector_search"
INDEX_NAME = "events_index"
LLM_MODEL = "databricks-meta-llama-3-1-70b-instruct"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define RAG Model Class

# COMMAND ----------

class EventRAGModel(mlflow.pyfunc.PythonModel):
    """
    MLflow wrapper for the Event RAG assistant
    """
    
    def __init__(self):
        self.catalog = CATALOG
        self.schema = SCHEMA
        self.vector_search_endpoint = VECTOR_SEARCH_ENDPOINT
        self.index_name = INDEX_NAME
        self.llm_model = LLM_MODEL
        self.vsc = None
        self.workspace_client = None
    
    def load_context(self, context):
        """Initialize clients when model loads"""
        self.vsc = VectorSearchClient(disable_notice=True)
        self.workspace_client = WorkspaceClient()
    
    def predict(self, context, model_input):
        """
        Process input questions and return answers
        
        Args:
            model_input: DataFrame with 'question' column
        
        Returns:
            DataFrame with 'answer' column
        """
        questions = model_input['question'].tolist()
        answers = []
        
        for question in questions:
            try:
                answer = self._ask_question(question)
                answers.append(answer)
            except Exception as e:
                answers.append(f"Error: {str(e)}")
        
        return pd.DataFrame({'answer': answers})
    
    def _ask_question(self, question: str, num_results: int = 5) -> str:
        """Process a single question through RAG pipeline"""
        
        # Step 1: Vector search for relevant events
        index = self.vsc.get_index(
            endpoint_name=self.vector_search_endpoint,
            index_name=f"{self.catalog}.{self.schema}.{self.index_name}"
        )
        
        search_results = index.similarity_search(
            query_text=question,
            columns=["event_name", "event_date", "venue_name", "city", "state", "event_text"],
            num_results=num_results
        )
        
        # Step 2: Format context from search results
        docs = search_results.get('result', {}).get('data_array', [])
        
        if not docs:
            return "I couldn't find any relevant events for your question. Try asking about concerts, sports, or entertainment events."
        
        formatted_events = []
        for i, doc in enumerate(docs):
            if isinstance(doc, (list, tuple)) and len(doc) >= 6:
                # doc[5] is event_text (last column)
                formatted_events.append(f"Event {i+1}: {doc[5]}")
        
        context = "\n\n".join(formatted_events)
        
        # Step 3: Generate response with LLM
        prompt = f"""You are a helpful assistant for Ticketmaster event information.

Context (Relevant Events):
{context}

User Question: {question}

Based on the events provided above, answer the user's question in a helpful and conversational way.
Include specific event details like names, dates, venues, and locations.

Answer:"""
        
        response = self.workspace_client.serving_endpoints.query(
            name=self.llm_model,
            messages=[
                ChatMessage(role=ChatMessageRole.SYSTEM, content="You are a helpful Ticketmaster event assistant."),
                ChatMessage(role=ChatMessageRole.USER, content=prompt)
            ],
            max_tokens=500,
            temperature=0.7
        )
        
        return response.choices[0].message.content

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test the Model Locally

# COMMAND ----------

# Create and test the model
model = EventRAGModel()
model.load_context(None)

# Test with sample questions
test_questions = pd.DataFrame({
    'question': [
        "What concerts are happening in Los Angeles?",
        "Show me sports events in New York"
    ]
})

results = model.predict(None, test_questions)
display(results)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register Model to MLflow

# COMMAND ----------

# Set up MLflow
mlflow.set_registry_uri("databricks-uc")
model_name = f"{CATALOG}.{SCHEMA}.event_rag_model"

# Log and register the model
with mlflow.start_run(run_name="event_rag_model") as run:
    
    # Log the model
    mlflow.pyfunc.log_model(
        artifact_path="model",
        python_model=EventRAGModel(),
        registered_model_name=model_name,
        signature=mlflow.models.signature.infer_signature(
            test_questions,
            results
        ),
        pip_requirements=[
            "databricks-vectorsearch",
            "databricks-sdk"
        ]
    )
    
    print(f"✓ Model registered: {model_name}")
    print(f"  Run ID: {run.info.run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploy as Model Serving Endpoint

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ServedEntityInput, EndpointCoreConfigInput

w = WorkspaceClient()

endpoint_name = "event-rag-assistant"

# Create or update endpoint
try:
    # Try to create new endpoint
    w.serving_endpoints.create(
        name=endpoint_name,
        config=EndpointCoreConfigInput(
            served_entities=[
                ServedEntityInput(
                    entity_name=model_name,
                    entity_version="1",
                    workload_size="Small",
                    scale_to_zero_enabled=True
                )
            ]
        )
    )
    print(f"✓ Created serving endpoint: {endpoint_name}")
    
except Exception as e:
    if "already exists" in str(e).lower():
        # Update existing endpoint
        w.serving_endpoints.update_config(
            name=endpoint_name,
            served_entities=[
                ServedEntityInput(
                    entity_name=model_name,
                    entity_version="1",
                    workload_size="Small",
                    scale_to_zero_enabled=True
                )
            ]
        )
        print(f"✓ Updated serving endpoint: {endpoint_name}")
    else:
        print(f"Error: {e}")

# COMMAND ----------

print(f"""
✅ Model Serving Endpoint Ready!

Endpoint Name: {endpoint_name}
Model: {model_name}

Test the endpoint:
  w = WorkspaceClient()
  response = w.serving_endpoints.query(
      name="{endpoint_name}",
      dataframe_records=[{{"question": "What concerts are in LA?"}}]
  )
""")

