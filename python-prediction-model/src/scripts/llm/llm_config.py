import ollama

def get_llm_response(model_name, prompt):
    """Sends a query to the specified LLM model and returns a response."""
    response = ollama.chat(model=model_name, messages=[{"role": "user", "content": prompt}])
    return response["message"]["content"].strip()