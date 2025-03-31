import json
import os

# --- Re-using definitions from above for a self-contained example ---
import typing
from typing import List, Optional

# from sqlalchemy.orm import Session
from sqlalchemy import Boolean, String, create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from src.scripts.data_warehouse.models.warehouse import Metrics, Session


def load_metrics_from_json(config_path: str):
    """Loads metric definitions from a JSON file into the database."""

    if not os.path.exists(config_path):
        print(f"Error: Configuration file not found at {config_path}")
        return

    try:
        with open(config_path, "r") as f:
            metrics_data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON from {config_path}: {e}")
        return
    except IOError as e:
        print(f"Error reading file {config_path}: {e}")
        return

    # Use a session to interact with the database
    with Session() as session:
        try:
            existing_ids = {m.id for m in session.query(Metrics.id).all()}
            new_metrics = []
            for metric_dict in metrics_data:
                metric_id = metric_dict.get("id")
                if metric_id is None:
                    print(
                        f"Skipping metric due to missing 'id': {metric_dict.get('metric_name', 'N/A')}")
                    continue

                if metric_id in existing_ids:
                    print(
                        f"Skipping metric with existing ID: {metric_id} ('{metric_dict.get('metric_name')}')")
                    # Optional: Update existing record instead
                    # existing_metric = session.get(Metrics, metric_id)
                    # if existing_metric:
                    #     for key, value in metric_dict.items():
                    #         setattr(existing_metric, key, value)
                    #     print(f"Updating metric ID: {metric_id}")
                    continue  # Skip adding if ID already exists

                # Create a Metrics object from the dictionary
                metric = Metrics(**metric_dict)
                new_metrics.append(metric)

            if new_metrics:
                session.add_all(new_metrics)
                session.commit()
                print(
                    f"Successfully added {len(new_metrics)} new metrics to the database.")
            else:
                print("No new metrics to add.")

        except Exception as e:
            print(f"An error occurred during database operation: {e}")
            session.rollback()  # Roll back changes on error


if __name__ == "__main__":
    # Example usage
    config_path = "/Users/bz/Developer/marines-data-analytics/src/scripts/data_warehouse/metric-config.json"
    load_metrics_from_json(config_path)
