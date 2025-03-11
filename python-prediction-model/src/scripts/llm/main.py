import os 
from dotenv import load_dotenv
import ollama 
from llm_config import get_llm_response

# Load model name from environment variable
dotenv_path = os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', '.env')
load_dotenv(dotenv_path)
MODEL_NAME = os.getenv("MODEL_NAME")

# Get LLM response
response = get_llm_response(MODEL_NAME, "Hello, how are you?")
print(response)
