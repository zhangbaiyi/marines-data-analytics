import calendar
import datetime
import io
import os
from typing import Any

import helpers.sidebar
from src.scripts.markdown_helper import build_markdown
import streamlit as st

from datetime import date
import calendar
from typing import List, Optional

import pandas as pd

from src.scripts.data_warehouse.access import (
    getMetricFromCategory,
    query_facts,
    getSites,
    getCamps,
)
from src.utils.logging import LOGGER
from src.scripts.data_warehouse.models.warehouse import SessionLocal
from src.scripts.pdf_helper import generate_pdf
from src.utils.logging import LOGGER




current_dir = os.path.dirname(os.path.abspath(__file__))
svg_path_250 = os.path.join(current_dir, "helpers", "static", "logo-250-years.svg")
svg_path_main = os.path.join(current_dir, "helpers", "static", "logo-mccs-white.svg")

# --- Display the first image in the first column ---
if os.path.exists(svg_path_main):
    _page_icon_path = svg_path_main
else:
    _page_icon_path = "🎖️"
    st.warning(f"Logo not found: {os.path.basename(svg_path_main)}")

st.set_page_config(page_title="MDAHub", page_icon=_page_icon_path, layout="wide")


def get_last_day_of_month(year: int, month: int) -> int:
    """Returns the last day of the month for a given year and month."""
    return calendar.monthrange(year, month)[1]


def build_month_list(
    start_year: int, start_month: int, end_year: int, end_month: int
) -> list[datetime.date]:
    """Returns a list of first-of-month dates from start → end (inclusive)."""
    months = []
    year, month = start_year, start_month
    while (year, month) <= (end_year, end_month):
        months.append(datetime.date(year, month, 1))
        # increment month
        if month == 12:
            month = 1
            year += 1
        else:
            month += 1
    return months


# -----------------------------------------------------------------------------#
# MAIN                                                                         #
# -----------------------------------------------------------------------------#
if __name__ == "__main__":
    helpers.sidebar.show()
    st.toast("Welcome to MDAHub", icon="🎖️")
    st.header("Fast Withdrawal")
    st.subheader("Choose category and month to get your report immediately.")

    # =====================  LEFT COLUMN – SELECTION  ========================= #
    selecter, dataviewer = st.columns([1, 3])
    with selecter:
        st.subheader("Inputs")
        st.markdown("#### Choose category")
        category_chosen = st.multiselect(
            "Select category(s)",
            ["Retail", "Email & Social Media", "Customer Survey"],
            help="Select one or multiple categories you want to analyze.",
            default=["Retail", "Customer Survey"],
        )

        st.divider()
        st.markdown("#### Choose month")

        # --------  Build static month list 2024-02 → 2025-01  ---------------- #
        month_options = build_month_list(2024, 2, 2025, 1)
        default_idx = len(month_options) - 1  # Default to last month
        selected_month = st.selectbox(
            "Select month (YYYY-MM)",
            options=month_options,
            index=default_idx,
            format_func=lambda d: d.strftime("%Y-%m"),
        )

        # -------------------  Enable / disable Confirm button  ---------------- #
        st.divider()
        confirm_disabled = not (selected_month and category_chosen)
        st.button(
            "Confirm",
            key="confirm_button",
            type="primary",
            disabled=confirm_disabled,
        )
        if confirm_disabled:
            st.caption(
                "Please select a month and at least one category to enable this button."
            )

    # =====================  RIGHT COLUMN – DATA VIEW  ======================== #
    with dataviewer:
        st.subheader("Data Viewer")
        st.write(
            "This is where you can view and analyze the selected data. "
            "You can apply various filters and transformations."
        )

        if st.session_state.get("confirm_button"):
            # ── Compute start / end dates for the chosen month ──────────────── #
            start_date = selected_month
            end_date = datetime.date(
                selected_month.year,
                selected_month.month,
                get_last_day_of_month(selected_month.year, selected_month.month),
            )

            with SessionLocal() as session:
                LOGGER.info("Database session opened.")

                # --- Fetch data ------------------------------------------------ #
                metric_ids = getMetricFromCategory(session=session, category=category_chosen)
                df = query_facts(
                    session=session,
                    metric_ids=metric_ids,
                    group_names=["all"],
                    period_levels=[2],  # monthly
                    date_from=start_date,
                    date_to=end_date,
                )
                # results = convert_jargons(session=session, df=df)

            # --- Build markdown -------------------------------------------------- #
            # markdown_output = io.StringIO()

            # # ── Level-1 title: month selected ──────────────────────────────────── #
            # report_month = selected_month.strftime("%Y-%m")          # e.g. 2025-01
            # markdown_output.write(f"# MCCS Data Analytics - {report_month}\n\n")


            # # Helper → translate the Metrics booleans into a category name
            # def _category_from_meta(meta: dict[str, Any]) -> str:
            #     if meta.get("is_retail"):
            #         return "Retail"
            #     if meta.get("is_marketing"):
            #         return "Email & Social Media"
            #     if meta.get("is_survey"):
            #         return "Customer Survey"
            #     return "Other"


            # # --------  bucket results by category -------------------------------- #
            # results_by_cat: dict[str, list[dict]] = {}
            # for metric_block in results.get("result", {}).values():
            #     meta = metric_block.get("metadata", {})
            #     cat = _category_from_meta(meta)
            #     results_by_cat.setdefault(cat, []).append(metric_block)

            # # --------  render markdown in the order the user chose -------------- #
            # for cat in category_chosen:                              # preserves UI order
            #     if cat not in results_by_cat:
            #         continue                                         # no data for this cat
            #     markdown_output.write(f"## {cat}\n\n")               # ── Level-2 title

            #     for entry in results_by_cat[cat]:
            #         meta      = entry["metadata"]
            #         all_data  = {k: v for k, v in entry.get("all", {}).items()
            #                     if k != "metadata"}

            #         metric_name = meta.get("metric_name", "N/A")
            #         metric_desc = meta.get("metric_desc", "No description")

            #         markdown_output.write(f"### {metric_name}\n\n")   # ── Level-3 title
            #         markdown_output.write(f"{metric_desc}\n\n")

            #         if all_data:
            #             period_key, value = next(iter(all_data.items()))
            #             formatted_val = (
            #                 f"{value:,.2f}" if isinstance(value, float) else f"{value:,}"
            #             )
            #             markdown_output.write(f"**Period:** {period_key}\n\n")
            #             markdown_output.write(f"**Value:** {formatted_val}\n\n")
            #         else:
            #             markdown_output.write("No data available for the period.\n\n")

            # markdown_string = markdown_output.getvalue()
            # markdown_output.close()

            markdown_string = build_markdown(session=session, year=selected_month.year, month=selected_month.month, categories=["retail", "customer_survey", "marketing"])

            # --- PDF Generation & download ----------------------------------- #
            if markdown_string.strip():
                st.write("Generating PDF report…")
                pdf_path = generate_pdf(_markdown=markdown_string)
                if pdf_path:
                    with open(pdf_path, "rb") as pdf_file:
                        st.download_button(
                            label="Download Report",
                            data=pdf_file.read(),
                            file_name="MDAHub_Report.pdf",
                            mime="application/pdf",
                        )
                else:
                    st.error("Failed to generate the PDF report.")
            else:
                st.warning("No data found to generate a report.")

            # --- Markdown preview ------------------------------------------- #
            st.markdown("### Report Preview (Markdown)")
            if markdown_string.strip():
                st.write(markdown_string)
            else:
                st.info("No content generated.")
        else:
            st.info("Make selections in the left panel and click **Confirm** to generate and view data.")
