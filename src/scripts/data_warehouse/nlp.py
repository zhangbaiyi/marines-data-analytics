import json
import sys
import time

import pandas as pd
from tqdm import tqdm
from transformers import pipeline

from src.scripts.utils import construct_path_from_project_root
from src.utils.logging import LOGGER

# Use default pipeline model ('distilbert-base-uncased-finetuned-sst-2-english')
MODEL_NAME = None

TOP_LEVEL_KEYS_TO_PROCESS = [
    "MainStores", "MarineMarts", "HospitalityServices", "FoodBeverage"]

TEXT_KEY = "answerFreeTextValues"
SENTIMENT_KEY = "sentiment"
SCORE_KEY = "sentiment_score"
RAW_LABEL_KEY = "original_model_label"


def survey_nlp_preprocess(_file_name: str) -> dict:
    excel_workbook = pd.read_excel(_file_name, sheet_name=None)
    customer_survey = {}
    for sheet_name, df_sheet in excel_workbook.items():
        if sheet_name == "Metadata":
            continue
        current_sheet = {}
        LOGGER.info(f"Processing Sheet: {sheet_name}")
        for index, row in tqdm(df_sheet.iterrows()):
            respondentId = row["respondentId"]
            questionLabel = row["questionLabel"]
            if respondentId not in current_sheet:
                current_sheet[respondentId] = {}
            if row["questionLabel"] and str(row["answerValues"]) != "nan":
                if questionLabel not in current_sheet[respondentId]:
                    current_sheet[respondentId][questionLabel] = row["answerValues"]
            if row["questionType"] == "CPP":
                current_sheet[respondentId]["storeid"] = row["answerFreeTextValues"]
                current_sheet[respondentId]["responseTime"] = row["responseTime"].strftime(
                    "%Y-%m-%d %H:%M:%S")
            elif str(row["answerFreeTextValues"]) != "nan":
                if "answerFreeTextValues" in current_sheet[respondentId]:
                    current_sheet[respondentId]["answerFreeTextValues"] += " " + \
                        str(row["answerFreeTextValues"])
                else:
                    current_sheet[respondentId]["answerFreeTextValues"] = str(
                        row["answerFreeTextValues"])
        customer_survey[sheet_name] = current_sheet
    return customer_survey


def survey_nlp_pipeline(_survey_dict: dict) -> dict:
    LOGGER.info(f"Python version: {sys.version}")
    start_time = time.time()
    LOGGER.info("--- Starting Sentiment Classification Process ---")
    LOGGER.info(f"Processing top-level keys: {TOP_LEVEL_KEYS_TO_PROCESS}")

    # 1. Load Data and Extract Texts
    # *** MODIFIED: Pass the list of keys and use the new mapping_info variable name ***
    original_json_data, texts_to_classify, mapping_info = load_and_extract_texts(
        _survey_dict, TOP_LEVEL_KEYS_TO_PROCESS, TEXT_KEY
    )

    if original_json_data and texts_to_classify:
        # 2. Load Model (only if there's text to classify)
        sentiment_classifier = load_sentiment_pipeline(MODEL_NAME)

        if sentiment_classifier:
            # 3. Classify Texts
            classification_results = classify_texts(
                sentiment_classifier, texts_to_classify)

            if classification_results:
                # 4. Add Labels back to the original structure
                # *** MODIFIED: Pass mapping_info ***
                final_labeled_data = add_labels_to_data(
                    original_json_data,
                    mapping_info,  # Use the updated mapping info
                    classification_results,
                    SENTIMENT_KEY,
                    SCORE_KEY,
                    RAW_LABEL_KEY,
                )

                if final_labeled_data:
                    return final_labeled_data
            else:
                LOGGER.info(
                    "Classification step failed or produced no results.")
        else:
            LOGGER.info("Model loading failed.")
    elif original_json_data is not None and not texts_to_classify:
        LOGGER.info(
            f"No text found under the '{TEXT_KEY}' key within the specified top-level keys ({TOP_LEVEL_KEYS_TO_PROCESS}) to classify."
        )
        LOGGER.info("Saving original data structure without modification.")
        return original_json_data
    else:
        LOGGER.info("Data loading failed.")

    end_time = time.time()
    LOGGER.info(
        f"--- Process Finished in {end_time - start_time:.2f} seconds ---")


def load_and_extract_texts(survey_dict, top_level_keys, text_key="answerFreeTextValues"):
    """
    Loads data from the specific JSON structure, iterates through specified top-level keys,
    extracts texts for classification, and keeps track of where to put the results back.

    Args:
        filepath (str): Path to the input JSON file.
        top_level_keys (list): A list of strings representing the top-level dictionary keys to process.
        text_key (str): The key within the inner dictionaries that holds the text value.

    Returns:
        tuple: (original_data, texts_to_classify, mapping_info)
               original_data: The fully loaded JSON data as a Python dictionary.
               texts_to_classify: A list of text strings found under the text_key.
               mapping_info: A list of tuples, where each tuple is (top_level_key, inner_key)
                             corresponding to each text in texts_to_classify, used to map results back.
               Returns (None, None, None) on error.
    """
    try:
        original_data = survey_dict

        texts_to_classify = []
        mapping_info = []

        LOGGER.info("Extracting texts for classification...")
        total_extracted_count = 0
        total_skipped_no_text = 0
        total_skipped_not_string = 0
        total_skipped_not_dict = 0

        for top_key in top_level_keys:
            LOGGER.info(f"--- Processing top-level key: '{top_key}' ---")
            if top_key not in original_data:
                LOGGER.info(
                    f"Warning: Top-level key '{top_key}' not found in the JSON. Skipping.")
                continue
            if not isinstance(original_data[top_key], dict):
                LOGGER.info(
                    f"Warning: Value for top-level key '{top_key}' is not a dictionary. Skipping.")
                continue

            # Iterate through the items (e.g., store_id, store_data) within the current top-level key
            for inner_key, item_data in original_data[top_key].items():
                if isinstance(item_data, dict):
                    if text_key in item_data:
                        text_value = item_data[text_key]
                        # Only classify non-empty strings
                        if isinstance(text_value, str) and text_value.strip():
                            texts_to_classify.append(text_value.strip())
                            # *** MODIFIED: Store both keys for mapping back ***
                            mapping_info.append((top_key, inner_key))
                            total_extracted_count += 1
                        elif not isinstance(text_value, str):
                            total_skipped_not_string += 1
                        else:  # Empty string
                            total_skipped_no_text += 1
                    else:
                        total_skipped_no_text += 1
                else:
                    LOGGER.info(
                        f"Warning: Value for key '{inner_key}' under '{top_key}' is not a dictionary. Skipping."
                    )
                    total_skipped_not_dict += 1

        LOGGER.info("-" * 20)  # Separator after processing all keys
        LOGGER.info(
            f"Successfully extracted {total_extracted_count} non-empty texts to classify across all specified keys."
        )
        if total_skipped_no_text > 0:
            LOGGER.info(
                f"Skipped {total_skipped_no_text} entries because '{text_key}' was missing or empty.")
        if total_skipped_not_string > 0:
            LOGGER.info(
                f"Skipped {total_skipped_not_string} entries because '{text_key}' was not a string.")
        if total_skipped_not_dict > 0:
            LOGGER.info(
                f"Skipped {total_skipped_not_dict} entries because the value was not a dictionary.")

        return original_data, texts_to_classify, mapping_info

    except FileNotFoundError:
        LOGGER.info(f"Error: Input JSON file not found at {filepath}")
        return None, None, None
    except json.JSONDecodeError:
        LOGGER.info(
            f"Error: Could not decode JSON from {filepath}. Check formatting.")
        return None, None, None
    except Exception as e:
        LOGGER.info(f"An unexpected error occurred during loading: {e}")
        return None, None, None


def load_sentiment_pipeline(model_name=None):
    """Loads the Hugging Face sentiment analysis pipeline."""
    try:
        LOGGER.info("Loading sentiment analysis model...")
        classifier = pipeline(
            "sentiment-analysis",
            model=model_name,
            # Originally -1, but prompting bus error on my macbook. Changing to 0 works.
            device=0,
        )
        LOGGER.info("Model loaded successfully.")
        return classifier
    except Exception as e:
        LOGGER.info(f"Error loading model pipeline: {e}")
        LOGGER.info(
            "Ensure 'transformers' and a backend ('torch' or 'tensorflow') are installed.")
        return None


def classify_texts(classifier, texts):
    if not classifier or not texts:
        LOGGER.info("Classifier is not loaded or no texts to classify.")
        return None
    try:
        LOGGER.info(f"Starting classification for {len(texts)} texts...")
        # The pipeline handles batching efficiently when given a list
        results = classifier(texts)
        LOGGER.info("Classification complete.")
        return results
    except Exception as e:
        LOGGER.info(f"An error occurred during classification: {e}")
        return None


def add_labels_to_data(original_data, mapping_info, classification_results, sentiment_key, score_key, raw_label_key):
    """
    Adds sentiment labels and scores back into the original nested data structure,
    using the mapping info to find the correct location.
    """
    # *** MODIFIED: Use mapping_info (list of tuples) ***
    if len(mapping_info) != len(classification_results):
        LOGGER.info(
            "Error: Mismatch between number of mapping info tuples and classification results.")
        LOGGER.info(
            f"Mapping info count: {len(mapping_info)}, Results count: {len(classification_results)}")
        return None  # Or return original_data without changes

    LOGGER.info("Adding labels back to the original data structure...")
    modified_data = original_data  # Work directly on the loaded data

    for i, mapping_tuple in enumerate(mapping_info):
        result = classification_results[i]
        try:
            top_level_key, inner_key = mapping_tuple
            target_dict = modified_data[top_level_key][inner_key]
            raw_label = result["label"]
            score = result["score"]
            sentiment_value = "unknown"

            if isinstance(raw_label, str):
                label_lower = raw_label.lower()
                # Common patterns
                if "positive" in label_lower or label_lower == "pos" or label_lower.endswith("_1"):
                    sentiment_value = "positive"
                # Common patterns
                elif "negative" in label_lower or label_lower == "neg" or label_lower.endswith("_0"):
                    sentiment_value = "negative"
                elif "neutral" in label_lower:  # Handle neutral if model supports it
                    sentiment_value = "neutral"

            target_dict[sentiment_key] = sentiment_value
            target_dict[score_key] = score
            target_dict[raw_label_key] = raw_label

        except KeyError:
            # *** MODIFIED: More informative error message ***
            LOGGER.info(
                f"Warning: Could not find key path '{top_level_key}' -> '{inner_key}' in original data when trying to add label. This shouldn't happen if loading was correct."
            )
        except Exception as e:
            # *** MODIFIED: More informative error message ***
            LOGGER.info(
                f"Warning: Error adding label for key path '{top_level_key}' -> '{inner_key}': {e}")

    LOGGER.info("Labels added.")
    return modified_data


# --- Main Execution ---
if __name__ == "__main__":
    file_name = "/Users/bz/Developer/MCCS Dataset/CustomerSurveyResponses.xlsx"
    json_data = survey_nlp_preprocess(file_name)
    enhanced_json_data = survey_nlp_pipeline(json_data)
    json_data = json.dumps(enhanced_json_data, indent=4)
    # Save the JSON data to a file
    with open(
        construct_path_from_project_root(
            "src/scripts/data_warehouse/customer_survey_responses_updated.json"), "w"
    ) as json_file:
        json_file.write(json_data)
