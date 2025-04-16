import fnmatch
import logging
import time
from pathlib import Path

import helpers.sidebar
import pandas as pd
import streamlit as st
from sqlalchemy import select
from streamlit.delta_generator import DeltaGenerator  # To type hint the container

import src.scripts.data_warehouse.etl as etl
from src.scripts.data_warehouse.models.warehouse import Metrics, SessionLocal
from src.scripts.data_warehouse.utils import (
    aggregate_metric_by_group_hierachy,
    aggregate_metric_by_time_period,
    insert_facts_from_df,
)
from src.utils.logging import LOGGER, StreamlitLogHandler

PROJECT_ROOT = Path(__file__).parent.parent
DATALAKE_DIR = PROJECT_ROOT / "datalake"


def get_etl_methods_for_pattern(pattern: str):
    """
    Determines which ETL methods/metrics to run based on the file pattern by
    querying the 'metrics' table in the database using metric IDs.
    Returns a list of tuples: (metric_name, etl_method, agg_method).
    """
    db = SessionLocal()
    etl_methods = []
    try:
        if pattern.startswith("RetailData"):
            metric_ids = [1, 2, 3, 4, 5, 6]  # Mapping metric IDs to RetailData
            time.sleep(0.5)
            for metric_id in metric_ids:
                query = select(Metrics.metric_name, Metrics.etl_method, Metrics.agg_method, Metrics.id).where(
                    Metrics.id == metric_id
                )
                result = db.execute(query).fetchone()
                if result:
                    etl_methods.append(
                        (result[0], result[1], result[2], result[3]))
                else:
                    LOGGER.warning(
                        f"Metric with ID '{metric_id}' not found in the database.")
        elif pattern.startswith("Advertising_Email_Deliveries"):
            metric_id = 7  # Assuming ID 7 corresponds to 'EmailDeliveries'
            if "st" in globals():
                st.write(f"-> Found metric with ID: '{metric_id}'")
            time.sleep(0.5)
            query = select(Metrics.metric_name, Metrics.etl_method, Metrics.agg_method, Metrics.id).where(
                Metrics.id == metric_id
            )
            result = db.execute(query).fetchone()
            if result:
                etl_methods.append(
                    (result[0], result[1], result[2], result[3]))
            else:
                LOGGER.warning(
                    f"Metric with ID '{metric_id}' not found in the database.")
        else:
            if "st" in globals():
                st.warning(
                    f"No specific ETL methods defined for pattern: {pattern}")
            LOGGER.warning(
                f"No specific ETL methods defined for pattern: {pattern}")

    finally:
        db.close()
    return etl_methods


def run_hydration_pipeline(uploaded_file, selected_pattern: str, output_container: DeltaGenerator):
    """
    Manages the ETL pipeline for the uploaded file.
    Args:
        uploaded_file: The file object from st.file_uploader.
        selected_pattern: The pattern string selected by the user.
        output_container: The Streamlit container (e.g., column) to display results.
    """
    output_container.empty()
    output_container.info(
        f"Starting hydration pipeline for **{uploaded_file.name}** (Pattern: **{selected_pattern}**)")

    streamlit_handler = StreamlitLogHandler(
        container=output_container, max_messages=5)
    log_format = "%(asctime)s - %(message)s"
    formatter = logging.Formatter(log_format, datefmt="%H:%M:%S")
    streamlit_handler.setFormatter(formatter)

    LOGGER.addHandler(streamlit_handler)

    try:
        datalake_path = DATALAKE_DIR / selected_pattern.replace("*", "")
        datalake_path.mkdir(parents=True, exist_ok=True)
        destination_file_path = datalake_path / uploaded_file.name

        with st.spinner(f"Uploading {uploaded_file.name} to {datalake_path}...", show_time=True):
            with open(destination_file_path, "wb") as f:
                f.write(uploaded_file.getbuffer())
            time.sleep(1)
        LOGGER.info(f"File successfully saved to: **{destination_file_path}**")
        destination_file_path_str = str(destination_file_path)

    except Exception as e:
        output_container.error(f"Error during file upload/saving: {e}")
        return

    etl_methods_to_run = get_etl_methods_for_pattern(selected_pattern)

    if not etl_methods_to_run:
        output_container.warning(
            "No ETL steps identified for this file pattern. Pipeline finished.")
        return
    LOGGER.info(
        f"Found {len(etl_methods_to_run)} metric(s) to process: {[m[0] for m in etl_methods_to_run]}")
    for metric_name, metric_etl_method_str, metric_agg_ethod_str, metric_id in etl_methods_to_run:
        metric_etl_method = getattr(etl, metric_etl_method_str)
        agg_method = metric_agg_ethod_str
        try:
            with st.spinner(f"Running ETL for {metric_name}...", show_time=True):
                lowest_level_df: pd.DataFrame = metric_etl_method(
                    destination_file_path_str)  # Pass path

            if lowest_level_df is None or lowest_level_df.empty:
                output_container.warning(
                    f"ETL for {metric_name} produced no data. Skipping downstream steps for this metric."
                )
                continue

            rows_inserted = insert_facts_from_df(lowest_level_df)
            if not rows_inserted:
                output_container.error(
                    f"Failed to insert facts for {metric_name}. Stopping pipeline for this metric.")
                continue
            if metric_name == "Inventory":
                agg_method = "last"
            if metric_name == "EmailDeliveries":
                agg_method = "count"

        except Exception as e:
            output_container.error(
                f"Error during ETL for metric **{metric_name}**: {e}")
    
    for metric_name, metric_etl_method_str, metric_agg_ethod_str, metric_id in etl_methods_to_run:
        agg_method = metric_agg_ethod_str
        try:
            with st.spinner(f"Aggregating {metric_name} by time ({agg_method})...", show_time=True):
                aggregated_df: pd.DataFrame = aggregate_metric_by_time_period(
                    _metric_id=int(metric_id), _method=agg_method)

            if aggregated_df is None or aggregated_df.empty:
                output_container.warning(
                    f"Time aggregation for {metric_name} produced no data. Skipping downstream steps."
                )
                continue

            with st.spinner(f"Inserting {metric_name} facts into database...", show_time=True):
                inserted_rows = insert_facts_from_df(aggregated_df)

            if not inserted_rows:
                output_container.error(
                    f"Failed to insert facts for {metric_name}. Stopping pipeline for this metric.")
                continue
            LOGGER.info(
                f"Inserted {inserted_rows} rows for {metric_name} into the database.")
        except Exception as e:
            output_container.error(
                f"Error processing metric **{metric_name}**: {e}")
    
    for metric_name, metric_etl_method_str, metric_agg_ethod_str, metric_id in etl_methods_to_run:
        agg_method = metric_agg_ethod_str
        try:
            with st.spinner(f"Performing hierarchical aggregation for {metric_name}...", show_time=True):
                hierarchy_df: pd.DataFrame = aggregate_metric_by_group_hierachy(
                    metric_id, agg_method)

            if hierarchy_df is not None:
                inserted_rows = insert_facts_from_df(hierarchy_df)
                if not inserted_rows:
                    output_container.error(
                        f"Failed to insert hierarchical facts for {metric_name}.")
                    continue
                LOGGER.info(
                    f"Inserted {inserted_rows} rows for hierarchical aggregation of {metric_name}.")
                output_container.success(
                    f"Metric **{metric_name}** processed successfully.")
            else:
                output_container.warning(
                    f"Hierarchical aggregation failed or was skipped for {metric_name}.")
        except Exception as e:
            output_container.error(
                f"Error processing metric **{metric_name}**: {e}")


# ── Page‑level settings ────────────────────────────────────────────────────────
st.set_page_config(
    page_title="hydrate",
    page_icon=":material/water_bottle_large:",
    layout="wide",
)

helpers.sidebar.show()
st.toast("Hydrate", icon=":material/water_bottle_large:")

st.header("Hydrate Data Lake")
st.subheader("Drag and drop a file to upload it to the Data Lake")

file_selector, hydrate_results = st.columns([1, 4])

with file_selector:
    st.markdown("##### Choose file")

    patterns = [
        "Advertising_Email_Deliveries*",
        "Advertising_Email_Engagement*",
        "Advertising_Email_Performance*",
        "CustomerSurveyResponses*",
        "RetailData*",
        "Social_Media_Performance*",
    ]

    selected_pattern = st.selectbox(
        "Select the file type pattern",
        options=patterns,
        index=4,  # Default to RetailData for example
        help="The uploaded file's name must match the selected pattern.",
    )

    uploaded_file = st.file_uploader(
        "Drag a file here or browse your computer",
        type=["xlsx", "parquet"],  # Allowed file extensions
        accept_multiple_files=False,
        label_visibility="visible",
    )

    valid_name = False
    if uploaded_file is not None:
        # Use fnmatch to check if the uploaded file name matches the selected pattern
        if fnmatch.fnmatch(uploaded_file.name, selected_pattern):
            valid_name = True
        else:
            st.warning(
                f"**{uploaded_file.name}** doesn't match the required pattern "
                f"**{selected_pattern}**. Please rename the file or choose the correct pattern."
            )

    # Only enable button if a file is uploaded AND its name is valid
    upload_clicked = st.button(
        "Upload and Run Pipeline",
        type="primary",
        disabled=not valid_name,  # Button disabled if no file or name mismatch
    )

# --- Results Column ---
with hydrate_results:
    st.markdown("##### Pipeline Progress & Results")
    # Create a container for pipeline output. Pass this to the pipeline function.
    results_container = st.container(border=False)
    results_container.write("Ready to hydrate!")


# --- Trigger Pipeline Execution ---
if upload_clicked and uploaded_file is not None and valid_name:
    # Call the pipeline function when the button is clicked and conditions are met
    with hydrate_results:  # Move the spinner to the hydrate_results column
        st.info(
            f"Pipeline initiated for {uploaded_file.name} (Pattern: {selected_pattern}). Check progress above.")
        run_hydration_pipeline(
            uploaded_file, selected_pattern, results_container)

        # Optional: Display a final success message outside the results container
        st.success(
            f"Pipeline completed for **{uploaded_file.name}** (Pattern: **{selected_pattern}**).")
