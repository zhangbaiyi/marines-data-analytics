import fnmatch
import json
import logging
import os
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
from src.scripts.utils import construct_path_from_project_root
from src.utils.logging import LOGGER, StreamlitLogHandler

PROJECT_ROOT = Path(__file__).parent.parent
DATALAKE_DIR = PROJECT_ROOT / "datalake"

if "pipeline_running" not in st.session_state:
    st.session_state["pipeline_running"] = False

if "pipeline_logs" not in st.session_state:
    st.session_state["pipeline_logs"] = []  # list[str]

if "last_uploaded" not in st.session_state:
    st.session_state["last_uploaded"] = None  # remembers last processed file

try:
    import torch  # noqa: F401
    torch_installed = True
    from src.scripts.data_warehouse.nlp import survey_nlp_pipeline, survey_nlp_preprocess
except ImportError:
    torch_installed = False
    LOGGER.warning("Torch is not installed. NLP modules will not be loaded.")
    survey_nlp_pipeline = None
    survey_nlp_preprocess = None


def _push_log(msg: str, level: str = "INFO") -> None:
    """Append a message to the session log cache and write to Streamlit."""
    st.session_state.pipeline_logs.append(f"[{level}] {msg}")
    st.write(msg)


def _reset_logs():
    st.session_state.pipeline_logs.clear()


def get_etl_methods_for_pattern(pattern: str):
    """Returns a list of tuples: (metric_name, etl_method, agg_method, metric_id)."""
    mapping = {
        "RetailData": [1, 2, 3, 4, 5, 6],
        "CustomerSurveyResponses": [7, 8, 20, 21, 22],
        "Advertising_Email_Deliveries": [18],
        "Advertising_Email_Engagement": [19],
        "Social_Media_Performance": [9, 10, 11, 12, 13, 14, 15, 16, 17],
    }
    key = next((k for k in mapping if pattern.startswith(k)), None)
    if key is None:
        LOGGER.warning("No specific ETL methods defined for pattern: %s", pattern)
        return []

    db = SessionLocal()
    try:
        ids = mapping[key]
        methods = []
        for metric_id in ids:
            name, etl_method, agg_method = db.execute(
                select(Metrics.metric_name, Metrics.etl_method, Metrics.agg_method).where(Metrics.id == metric_id)
            ).fetchone()
            methods.append((name, etl_method, agg_method, metric_id))
        return methods
    finally:
        db.close()



def run_hydration_pipeline(uploaded_file, selected_pattern: str, output_container: DeltaGenerator):
    """Runs the ETL + aggregation pipeline and streams logs to *output_container*."""
    # ‑‑ Reset state for a fresh run
    _reset_logs()
    st.session_state.pipeline_running = True
    st.session_state.last_uploaded = uploaded_file.name

    # Attach a Streamlit log handler that prints to *output_container* **and**
    # stores each log line in session state so it survives implicit re‑runs.
    streamlit_handler = StreamlitLogHandler(container=output_container, max_messages=5)
    formatter = logging.Formatter("%(asctime)s ‑ %(message)s", datefmt="%H:%M:%S")
    streamlit_handler.setFormatter(formatter)
    LOGGER.addHandler(streamlit_handler)

    try:
        # ───────────────────── 1. Save the uploaded file ──────────────────────
        datalake_path = DATALAKE_DIR / selected_pattern.replace("*", "")
        datalake_path.mkdir(parents=True, exist_ok=True)
        destination = datalake_path / uploaded_file.name

        with st.spinner(f"Uploading {uploaded_file.name} → {datalake_path}…"):
            destination.write_bytes(uploaded_file.getbuffer())
            time.sleep(0.3)
        LOGGER.info("File saved to %s", destination)

        # ───────────────── 2. Special NLP for survey files (optional) ─────────
        destination_path = str(destination)
        if selected_pattern.startswith("CustomerSurveyResponses"):
            if torch_installed and survey_nlp_preprocess and survey_nlp_pipeline:
                json_data = survey_nlp_preprocess(destination_path)
                enhanced = survey_nlp_pipeline(json_data)
                LOGGER.info("NLP enriched %d survey records", len(enhanced))
                json_out = destination.with_suffix(".json")
                json_out.write_text(json.dumps(enhanced, indent=4))
                destination_path = str(json_out)
                LOGGER.info("JSON written to %s", json_out)
            else:
                st.warning("Torch missing – skipping survey NLP step.")

        # ───────────────────── 3. Determine ETL steps ────────────────────────
        etl_steps = get_etl_methods_for_pattern(selected_pattern)
        if not etl_steps:
            output_container.warning("No ETL steps found for this pattern – stopping.")
            return
        LOGGER.info("%d metric(s) to process: %s", len(etl_steps), [s[0] for s in etl_steps])

        # ───────────────────── 4. Execute ETL for each metric ─────────────────
        for metric_name, etl_fn_str, agg_method, metric_id in etl_steps:
            etl_fn = getattr(etl, etl_fn_str)
            with st.spinner(f"ETL → {metric_name} …"):
                lowest_df: pd.DataFrame = etl_fn(destination_path)
            if lowest_df is None or lowest_df.empty:
                output_container.warning(f"ETL for {metric_name} yielded no data – skipping.")
                continue
            inserted = insert_facts_from_df(lowest_df)
            LOGGER.info("Inserted %s raw rows for %s", inserted, metric_name)

        # ─────────────────── 5. Time aggregation & DB insert ──────────────────
        for metric_name, _, agg_method, metric_id in etl_steps:
            with st.spinner(f"Time aggregation ({agg_method}) → {metric_name} …"):
                time_df = aggregate_metric_by_time_period(metric_id, agg_method)
            if time_df.empty:
                output_container.warning(f"No time aggregates for {metric_name}")
                continue
            inserted = insert_facts_from_df(time_df)
            LOGGER.info("Inserted %s time‑agg rows for %s", inserted, metric_name)

        # ──────────────── 6. Hierarchical aggregation (except id=9) ───────────
        for metric_name, _, agg_method, metric_id in etl_steps:
            if metric_id == 9:
                continue
            with st.spinner(f"Hierarchy aggregation → {metric_name} …"):
                hier_df = aggregate_metric_by_group_hierachy(metric_id, agg_method)
            if hier_df.empty:
                continue
            inserted = insert_facts_from_df(hier_df)
            LOGGER.info("Inserted %s hierarchy rows for %s", inserted, metric_name)
            output_container.success(f"Metric {metric_name} processed ✔️")

        # ─────────────────── 7. Clean up temporary file ───────────────────────
        try:
            destination.unlink(missing_ok=True)
            LOGGER.info("Temp file %s deleted", destination)
        except OSError as e:
            LOGGER.error("File deletion failed: %s", e)

        output_container.success(
            f"✅ Pipeline finished for **{uploaded_file.name}** ({selected_pattern}). Results stay visible until the next run or page refresh."
        )
    finally:
        LOGGER.removeHandler(streamlit_handler)
        st.session_state.pipeline_running = False

current_dir = Path(__file__).parent
svg_path_main = current_dir / ".." / "helpers" / "static" / "logo-mccs-white.svg"

st.set_page_config(
    page_title="Hydrate Data Lake",
    page_icon=str(svg_path_main) if svg_path_main.exists() else ":material/water_bottle_large:",
    layout="wide",
)

helpers.sidebar.show()

st.header("Hydrate Data Lake")
st.subheader("Drag & drop a file to upload it into the lake")

# Layout: selector column | results column
selector_col, results_col = st.columns([1, 4])

with selector_col:
    st.markdown("##### Choose file")
    patterns = [
        "Advertising_Email_Deliveries*",
        "Advertising_Email_Engagement*",
        "CustomerSurveyResponses*",
        "RetailData*",
        "Social_Media_Performance*",
    ]
    selected_pattern = st.selectbox("File pattern", patterns, index=3)
    uploaded_file = st.file_uploader("Drag a file here or browse", type=["xlsx", "parquet"])

    valid_name = uploaded_file and fnmatch.fnmatch(uploaded_file.name, selected_pattern)
    if uploaded_file and not valid_name:
        st.warning(f"{uploaded_file.name} ≠ pattern {selected_pattern}. Please rename or pick correct pattern.")

    if st.button("Upload & Run", type="primary", disabled=not valid_name):
        with results_col:
            run_hydration_pipeline(uploaded_file, selected_pattern, st)
            st.toast("Pipeline completed – see logs above.")

with results_col:
    st.markdown("##### Pipeline Progress & Results")

    for line in st.session_state.pipeline_logs:
        st.write(line)

    if not st.session_state.pipeline_logs:
        st.info("Ready to hydrate!")
