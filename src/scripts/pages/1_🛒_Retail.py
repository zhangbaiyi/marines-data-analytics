import helpers.sidebar
import streamlit as st

from src.scripts.data_warehouse.access import getCamps, getMetricByID, getMetricFromCategory, getSites
from src.scripts.data_warehouse.models.warehouse import get_db

st.set_page_config(page_title="Retail",
                   page_icon=":material/storefront:", layout="wide")


helpers.sidebar.show()
# Display a toast notification
st.header("Retail Insights")

data_visualization, menu_selection = st.columns([2,1])
with data_visualization:
    st.subheader("Performance Metrics")
    db = next(get_db())
    retail_metrics_ids = getMetricFromCategory(db, category=["Retail"])
    metric_names = []
    if retail_metrics_ids:
        for metric_id in retail_metrics_ids:
            metric_data = getMetricByID(db, metric_id)
            if metric_data and 'metric_name' in metric_data:
                metric_names.append(metric_data['metric_name'].title())
            else:
                metric_names.append(f"Metric Id: {metric_id}")
    else:
        metric_names = ["No Retail Metrics Found"]

    st.tabs(metric_names)


with menu_selection:
    st.subheader("Target Selection")

    db = next(get_db())

    # 1. Marine mart or marine exchange
    all_sites_data = getSites(db)
    unique_store_formats = ['MAIN STORE', 'MARINE MART']
    store_formats = [fmt.upper() for fmt in unique_store_formats if fmt]
    selected_format = st.pills("Select Store Format", [fmt.title() for fmt in store_formats], default=None)

    # Filter sites based on the selected store format
    filtered_sites_data = []
    if selected_format:
        filtered_sites_data = [
            site for site in all_sites_data if site.store_format and site.store_format == selected_format.upper()
        ]
    else:
        filtered_sites_data = all_sites_data

    # 2. Camp A, Camp B, ... (default selection is None)
    camps_data = getCamps(db)
    camp_names = sorted([camp.name for camp in camps_data if camp.name]) if camps_data else []
    selected_camp = st.multiselect("Select Camp(s)", [name.title() for name in camp_names], default=[])

    # Filter sites based on selected camp(s)
    if selected_camp:
        filtered_sites_data = [
            site for site in filtered_sites_data
            if site.command_name and site.command_name.upper() in [sc.upper() for sc in selected_camp]
        ]
    else:
        filtered_sites_data = filtered_sites_data

    # 3. Site A, Site B... (dynamically updated based on store format and camp, default selection is None)
    site_names = []
    if filtered_sites_data:
        unique_site_names = sorted(list(set(site.site_name for site in filtered_sites_data if site.site_name)))
        site_names = [name.title() for name in unique_site_names]
    selected_site = st.multiselect("Select Site(s)", site_names, default=[])

    # Further filter based on selected site(s)
    if selected_site:
        filtered_sites_data = [
            site for site in filtered_sites_data
            if site.site_name and site.site_name.upper() in [ss.upper() for ss in selected_site]
        ]

    st.button("Submit")