import os

import ollama
from dotenv import load_dotenv

from src.scripts.llm.llm_config import get_llm_response
from src.scripts.llm.prompts import generate_analysis_prompt
from src.scripts.utils import resolve_import_path_from_project_root
from src.utils.logging import LOGGER

# Load model name from environment variable
dotenv_path = resolve_import_path_from_project_root(".env")
load_dotenv(dotenv_path)
MODEL_NAME = os.getenv("MODEL_NAME") or ""

# Method to choose the file from the file type : TODO in another file when we have the datasets
# def choose_file(file_type):
#     if file_type == "sales":
#         return "sales.csv"
#     elif file_type == "marketing":
#         return "marketing.csv"
#     elif file_type == "finance":
#         return "finance.csv"
#     else:
#         return "data.csv"

if __name__ == "__main__":
    data = [  # Dummy data
        {
            "file_name": "Marine Mart Sales Report",
            "file_type": "Total Sales",
            "area": "Henderson Hall Main Store",
            "value": 2224948.54,
            "time_period": "December 2024",
        },
        {
            "file_name": "Customer Feedback",
            "file_type": "Customer Reviews",
            "area": "HHM MCX Main Store",
            "value": 89.7,
            "time_period": "Year 2024",
        },
        {
            "file_name": "Quarterly Email Metrics",
            "file_type": "Email",
            "area": "Regional Distribution Center",
            "value": 150000,
            "time_period": "Q4 2024",
        },
    ]

    # Example usage with the first entry from the dummy data
    example_data = data[0]
    prompt = generate_analysis_prompt(
        file_name=example_data["file_name"],
        file_type=example_data["file_type"],
        area=example_data["area"],
        value=example_data["value"],
        time_period=example_data["time_period"],
    )

    # Get LLM response
    response = get_llm_response(MODEL_NAME, prompt)
    LOGGER.info(f"Response from LLM: {response}")
