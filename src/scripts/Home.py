import calendar
import datetime
import io
import os

import helpers.sidebar
import streamlit as st

from src.scripts.data_warehouse.access import convert_jargons, getMetricFromCategory, query_facts
from src.scripts.data_warehouse.models.warehouse import SessionLocal
from src.scripts.pdf_helper import generate_pdf
from src.utils.logging import LOGGER

current_dir = os.path.dirname(os.path.abspath(__file__))
svg_path_250 = os.path.join(
    current_dir, "helpers", "static", "logo-250-years.svg")
svg_path_main = os.path.join(
    current_dir, "helpers", "static", "logo-mccs-white.svg")

# --- Display the first image in the first column ---
if os.path.exists(svg_path_main):
    _page_icon_path = svg_path_main
else:
    _page_icon_path = "🎖️"
    st.warning(f"Logo not found: {os.path.basename(svg_path_main)}")

st.set_page_config(page_title="MDAHub", page_icon=_page_icon_path, layout="wide")




def get_last_day_of_month(year, month):
    """Returns the last day of the month for a given year and month."""
    return calendar.monthrange(year, month)[1]



if __name__ == "__main__":
    helpers.sidebar.show()
    st.toast("Welcome to MDAHub", icon="🎖️")
    st.header("Fast Withdrawal")
    st.subheader("Choose category and date range to get your report immediately.")

    # Columns
    selecter, dataviewer = st.columns([1, 3])
    with selecter:
        st.subheader("Inputs")
        st.markdown("#### Choose category")
        category_chosen = st.multiselect(
            "Select category(s)",
            [
                "Retail",
                "Email & Social Media",
                "Customer Survey",
            ],
            help="Select one or multiple categories you want to analyze.",
        )

        st.divider()
        st.markdown("#### Choose date")

        today = datetime.date.today()
        first_day_current_month = today.replace(day=1)
        # last_day_prev_month = first_day_current_month - datetime.timedelta(days=1)
        # first_day_prev_month = last_day_prev_month.replace(day=1)
        # default : 2024-01-01  2025-01-31
        default_start_date = datetime.date(2024, 1, 1)
        default_end_date = datetime.date(2025, 1, 31)
        default_range = (default_start_date, default_end_date)
        daterange_chosen = st.date_input(
            "Select Date Range",
            value=default_range,
            help="Select the start and end date. We will check if it covers full months.",
        )

        if isinstance(daterange_chosen, tuple) and len(daterange_chosen) == 2:
            start_date, end_date = daterange_chosen

            st.write(
                f"You have selected the range: **{start_date}** to **{end_date}**")
            is_start_first_day = start_date.day == 1
            last_day_of_end_month = get_last_day_of_month(
                end_date.year, end_date.month)
            is_end_last_day = end_date.day == last_day_of_end_month

            if is_start_first_day and is_end_last_day:
                if end_date >= start_date:
                    st.success(
                        "✅ The selected range represents one or more complete months.")

                else:
                    st.error("❌ Error: The end date cannot be before the start date.")

            else:
                st.warning(
                    "⚠️ The selected range does **not** represent full month(s).")

        elif daterange_chosen:
            st.warning(
                "Please select a valid date *range* (both start and end dates).")
            st.write("Currently selected:", daterange_chosen)
        else:
            st.info("Select a date range using the date picker above.")

        is_valid_full_month = False
        if isinstance(daterange_chosen, tuple) and len(daterange_chosen) == 2:
            start_date, end_date = daterange_chosen
            is_start_first_day = start_date.day == 1
            last_day_of_end_month = get_last_day_of_month(
                end_date.year, end_date.month)
            is_end_last_day = end_date.day == last_day_of_end_month
            if is_start_first_day and is_end_last_day and end_date >= start_date:
                is_valid_full_month = True

        st.divider()

        if is_valid_full_month and category_chosen:
            st.button("Confirm", key="confirm_button", type="primary")
        else:
            st.button("Confirm", key="confirm_button",
                    type="primary", disabled=True)
            st.caption(
                "Please select a valid date range and at least one category to enable this button.")


    with dataviewer:
        st.subheader("Data Viewer")
        st.write(
            "This is where you can view and analyze the selected data. You can apply various filters and transformations."
        )
        if confirm_button := st.session_state.get("confirm_button"):
            if confirm_button:
                with SessionLocal() as session:
                    LOGGER.info("Database session opened.")
                    # --- Your data fetching logic remains the same ---
                    metric_ids = getMetricFromCategory(
                        session=session, category=category_chosen)
                    df = query_facts(
                        session=session,
                        metric_ids=metric_ids,
                        group_names=["all"],
                        period_levels=[2],
                        date_from=daterange_chosen[0],
                        date_to=daterange_chosen[1],
                    )
                    results = convert_jargons(session=session, df=df)

                    # --- Your markdown generation logic remains the same ---
                    markdown_output = io.StringIO()
                    for key, item in results.get("result", {}).items():
                        metadata = item.get("metadata", {})
                        all_data = item.get("all", {})
                        metric_name = metadata.get("metric_name", "N/A")
                        metric_desc = metadata.get("metric_desc", "No description")
                        markdown_output.write(f"# Report\n\n")
                        markdown_output.write(f"## {metric_name}\n\n")
                        markdown_output.write(f"{metric_desc}\n\n")
                        period_key = None
                        value = None
                        for data_key, data_value in all_data.items():
                            if data_key != "metadata":
                                period_key = data_key
                                value = data_value
                                break
                        if period_key is not None and value is not None:
                            formatted_value = f"{value:,.2f}" if isinstance(
                                value, float) else f"{value:,}"
                            markdown_output.write(f"**Period:** {period_key}\n")
                            markdown_output.write(
                                f"**Value:** {formatted_value}\n\n")
                        else:
                            markdown_output.write(
                                "No data available for the period.\n\n")
                    markdown_string = markdown_output.getvalue()
                    markdown_output.close()

                    # --- PDF Generation and Download Button ---
                    if len(markdown_string.strip()) > 0:  # Check if markdown is not just whitespace
                        st.write("Generating PDF report...")  # Give user feedback
                        pdf_file_path_on_server = generate_pdf(
                            _markdown=markdown_string)

                        if pdf_file_path_on_server:  # Check if PDF generation succeeded
                            try:
                                # Read the generated PDF file as bytes
                                with open(pdf_file_path_on_server, "rb") as pdf_file:
                                    pdf_bytes = pdf_file.read()

                                # Provide the bytes to the download button
                                st.download_button(
                                    label="Download Report",
                                    # <-- Pass the actual PDF content (bytes)
                                    data=pdf_bytes,
                                    file_name="MDAHub_Report.pdf",  # <-- User-friendly download name
                                    mime="application/pdf",
                                )
                                # Optional: Clean up the generated file on the server if you don't need it anymore
                                # try:
                                #     os.remove(pdf_file_path_on_server)
                                #     LOGGER.info(f"Cleaned up temporary PDF: {pdf_file_path_on_server}")
                                # except OSError as e:
                                #     LOGGER.warning(f"Could not remove temporary PDF {pdf_file_path_on_server}: {e}")

                            except FileNotFoundError:
                                st.error(
                                    f"Error: Could not find the generated PDF file.")
                                LOGGER.error(
                                    f"FileNotFoundError trying to read {pdf_file_path_on_server} for download.")
                            except Exception as e:
                                st.error(f"Error preparing PDF for download: {e}")
                                LOGGER.error(
                                    f"Error reading PDF file {pdf_file_path_on_server} for download: {e}", exc_info=True
                                )
                        else:
                            st.error("Failed to generate the PDF report.")
                    else:
                        st.warning("No data found to generate a report.")

                    # Display the generated markdown in the app as well
                    # Add a header for clarity
                    st.markdown("### Report Preview (Markdown)")
                    if len(markdown_string.strip()) > 0:
                        st.write(markdown_string)
                    else:
                        st.info("No content generated.")

            else:
                # This condition seems unreachable if the outer `if confirm_button:` is True.
                # Maybe you intended this for when the button hasn't been clicked yet?
                # If so, move it outside the `if confirm_button:` block.
                # st.write("Please make a selection in the left panel and click Confirm.")
                pass  # This block likely isn't needed here if the outer `if` handles the button state

        else:  # This runs if the confirm button hasn't been clicked or is False in session state
            st.info(
                "Make selections in the left panel and click 'Confirm' to generate and view data.")
