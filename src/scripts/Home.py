import json
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, NamedTuple, cast

import streamlit as st
import helpers.sidebar

from src.scripts.data_warehouse.access import convert_jargons, getMetricFromCategory, query_facts
from src.scripts.data_warehouse.models.warehouse import CustomJSONEncoder, Session
from src.scripts.pdf_demo import generate_pdf
from src.utils.logging import LOGGER

# Global Counter
num_request = 0


# CONSTANTS
@dataclass
class CONSTANTS(NamedTuple):
    RABBITMQ_URL = "amqp://guest:guest@localhost:5672/"
    CHANNEL_QUEUE_PREFIX = "file_generate_status"
    ML_MODEL_FALLBACK_TOKEN_RESULT = "<NO-RESULT>"


# TYPES
class PredictionDict(NamedTuple):
    file_name: str


def predict(contents: Dict) -> PredictionDict:
    LOGGER.debug(f"Contents: {contents}")
    value: str = contents.get("value")
    query: Dict = contents.get("query_params")
    LOGGER.debug(value)
    LOGGER.debug(query)
    query_types: List[str] = cast(str, query.get("category")).split(",")
    LOGGER.debug(query_types)
    month_selected = cast(int, query.get("month"))
    LOGGER.debug(month_selected)
    group = cast(str, query.get("group"))
    LOGGER.debug(group)

    # Add your code logic for data processing, AI Agent, and PDF generation here
    return_content = process_request(contents=contents)
    return_file_name = generate_pdf(_markdown=f"{return_content}")

    return {"file_name": (return_file_name if len(return_file_name) > 0 else CONSTANTS.ML_MODEL_FALLBACK_TOKEN_RESULT)}


session = Session()


def process_request(contents: Dict) -> str:
    LOGGER.debug(f"Contents: {contents}")
    query: Dict = contents.get("query_params")
    LOGGER.debug(query)
    category_types: List[str] = cast(str, query.get("category")).split(",")
    LOGGER.debug(category_types)
    month_selected = cast(str, query.get("month"))
    LOGGER.debug(month_selected)
    group: List[str] = cast(str, query.get("group")).split(",")
    LOGGER.debug(group)
    metric_ids = getMetricFromCategory(
        session=session, category=category_types)
    date_selected = datetime.strptime(month_selected, "%Y%m").date()
    warehouse_result = query_facts(
        session=session,
        metric_ids=metric_ids,
        group_names=group,
        period_level=2,
        exact_date=date_selected,
    )
    translated_data = convert_jargons(df=warehouse_result, session=session)
    return json.dumps(translated_data, cls=CustomJSONEncoder)


def main() -> None:
    st.set_page_config(
	page_title="MDAHub",
	page_icon="🎖️",
	layout="wide"

    )

    helpers.sidebar.show()
    st.toast("Welcome to MDAHub", icon="🎖️")
    st.markdown("Welcome to the all-in-one data analytics solution for MCCS!")
    st.write("Explore different kinds of data analytics features from the sidebar!")
    return None


if __name__ == "__main__":
    main()
