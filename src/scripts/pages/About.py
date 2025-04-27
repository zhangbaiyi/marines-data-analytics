import os

import helpers.sidebar
import streamlit as st

current_dir = os.path.dirname(os.path.abspath(__file__))
svg_path_250 = os.path.join(
    current_dir, "..", "helpers", "static", "logo-250-years.svg")
svg_path_main = os.path.join(
    current_dir, "..", "helpers", "static", "logo-mccs-white.svg")

# --- Determine the page icon ---
if os.path.exists(svg_path_main):
    _page_icon_path = svg_path_main
else:
    _page_icon_path = ":material/info:"
    st.warning(f"Logo not found: {os.path.basename(svg_path_main)}")

st.set_page_config(page_title="About",
                   page_icon=_page_icon_path, layout="wide")

helpers.sidebar.show()

# Toast notification when the page loads
st.toast("About MDAHub", icon=":material/info:")

# ──────────────────────────────────────────────────────────────────────────────
# Main About content
# ──────────────────────────────────────────────────────────────────────────────

st.markdown(
    """
    ## Welcome to **MDAHub**

    MDAHub is the United States Marine Corps Community Services’ central hub for retail, marketing, and survey analytics.

    **Key things you can do here**

    - **Retail Insights** – Drill into sales KPIs, compare stores, and visualise performance on an interactive map.
    - **Marketing** – Track social media and email campaign metrics in one place.
    - **Customer Survey** – Get insights of the customer's comments and survey scores.
    - **Hydrate Data Lake** – Refresh the underlying data‑warehouse with a single click.

    **Version**: 1.0.0  
    **Author**: Baiyi Zhang  
    **Contact**: baiyi@vt.edu

    ---
    _You don't **join** the Marines, you **become** one._
    """
)
