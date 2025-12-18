"""
Event RAG Web Application for Databricks Apps
Simple web interface to query the RAG model serving endpoint
"""

import gradio as gr
from databricks.sdk import WorkspaceClient
import os

# Configuration
ENDPOINT_NAME = "event-rag-assistant"

# Initialize Databricks client
w = WorkspaceClient()

def ask_event_question(question: str) -> str:
    """
    Send question to RAG endpoint and return answer
    
    Args:
        question: User's question about events
    
    Returns:
        Answer from RAG model
    """
    if not question.strip():
        return "Please enter a question about events."
    
    try:
        # Query the serving endpoint
        response = w.serving_endpoints.query(
            name=ENDPOINT_NAME,
            dataframe_records=[{"question": question}]
        )
        
        # Extract answer from response
        predictions = response.predictions
        if predictions and len(predictions) > 0:
            return predictions[0]['answer']
        else:
            return "No response from model. Please try again."
            
    except Exception as e:
        return f"Error querying endpoint: {str(e)}\n\nMake sure the endpoint '{ENDPOINT_NAME}' is deployed and ready."

# Example questions for users
examples = [
    ["What concerts are happening in Los Angeles this weekend?"],
    ["Show me sports events in New York"],
    ["Are there any rock concerts under $100?"],
    ["What are the most popular venues?"],
    ["Find family-friendly events in California"]
]

# Create Gradio interface
demo = gr.Interface(
    fn=ask_event_question,
    inputs=gr.Textbox(
        label="Ask about events",
        placeholder="What concerts are in LA?",
        lines=2
    ),
    outputs=gr.Textbox(
        label="Answer",
        lines=10
    ),
    title="🎭 Ticketmaster Event Assistant",
    description="""
    Ask questions about upcoming events, concerts, sports, and entertainment.
    Powered by RAG (Retrieval Augmented Generation) with Vector Search and LLM.
    """,
    examples=examples,
    theme=gr.themes.Soft(),
    allow_flagging="never"
)

if __name__ == "__main__":
    # Launch the app for Databricks Apps
    # Databricks Apps expects the app to listen on 0.0.0.0:8080
    demo.launch(
        server_name="0.0.0.0",
        server_port=8080,
        share=False
    )

