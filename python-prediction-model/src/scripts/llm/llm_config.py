import ollama


def get_llm_response(model_name: str, prompt: str) -> str:
    """Sends a query to the specified LLM model and returns a response."""
    assert len(model_name) > 0, "Error: The model name must be specified."
    assert len(prompt) > 0, "Error: The prompt must be specified."
    response = ollama.chat(model=model_name, messages=[
                           {"role": "user", "content": prompt}])
    return str(response["message"]["content"]).strip()
