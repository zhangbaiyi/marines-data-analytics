import os 
from dotenv import load_dotenv
import ollama 
from src.scripts.llm.llm_config import get_llm_response
from src.scripts.utils import resolve_import_path_from_project_root

# Load model name from environment variable
dotenv_path = resolve_import_path_from_project_root(".env")
load_dotenv(dotenv_path)
MODEL_NAME = os.getenv("MODEL_NAME") or ""

# Get LLM response
response = get_llm_response(MODEL_NAME, "Hello, how are you?")
print(response)
