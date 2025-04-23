import os
import plotly.express as px
import altair as alt
import pandas as pd
alt.data_transformers.disable_max_rows()
import helpers.sidebar
import streamlit as st

from src.scripts.data_warehouse.access import getCamps, getMetricByID, getMetricFromCategory, getSites, query_facts
from src.scripts.data_warehouse.models.warehouse import get_db

current_dir = os.path.dirname(os.path.abspath(__file__))
svg_path_250 = os.path.join(
    current_dir, "..", "helpers", "static", "logo-250-years.svg")
svg_path_main = os.path.join(
    current_dir, "..", "helpers", "static", "logo-mccs-white.svg")

# --- Display the first image in the first column ---
if os.path.exists(svg_path_250):
    _page_icon_path = svg_path_250
else:
    _page_icon_path = ":material/storefront:"
    st.warning(f"Logo not found: {os.path.basename(svg_path_250)}")

st.set_page_config(page_title="Retail Insights",
                   page_icon= _page_icon_path, layout="wide")

if __name__ == "__main__":
    helpers.sidebar.show()
    # Display a toast notification
    st.header("Retail Insights")

    data_visualization, menu_selection = st.columns([2,1])

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
        if filtered_sites_data:
            # Build an ID → Name lookup from the filtered sites
            id_to_name = {
                site.site_id: site.site_name.title()           # {123: "Camp Pendleton"}
                for site in filtered_sites_data
                if site.site_id and site.site_name
            }
            site_options = sorted(id_to_name.keys())           # [123, 456, …]

            selected_site_ids = st.multiselect(
                "Select Site(s)",
                site_options,                                   # the *values* returned
                default=[],
                format_func=lambda sid: id_to_name[sid]         # what the user sees
            )
        else:
            selected_site_ids = []

        # Filter again using the chosen IDs
        if selected_site_ids:
            filtered_sites_data = [
                site for site in filtered_sites_data
                if site.site_id in selected_site_ids
            ]

        PERIOD_LEVELS = {
            "Daily": 1,
            "Monthly": 2,
            "Quarterly": 3,
            "Yearly": 4
        }
        selected_period_label = st.selectbox(
            "Select Period",
            options=list(PERIOD_LEVELS.keys()),
            index=2  # Default to Monthly
        )
        selected_period_level = PERIOD_LEVELS[selected_period_label]

        st.button("Submit")

    with data_visualization:
        st.subheader("Performance Metrics")

        db = next(get_db())

        # 1️⃣  Pull all retail metric‑ids & names
        retail_metric_ids = getMetricFromCategory(db, category=["Retail"])
        metric_names = []
        for m_id in retail_metric_ids:
            m = getMetricByID(db, m_id)
            metric_names.append(m["metric_name"].title() if m else f"Metric ID {m_id}")

        # 2️⃣  Build <tab> objects, one per metric
        tabs = st.tabs(metric_names)
        id_lookup = dict(zip(metric_names, retail_metric_ids))          # name → id

        # 3️⃣  Helper: decide which group_name(s) we should query for
        def _active_group_names() -> list[str]:
            if selected_site_ids:                     # now a list[int]
                return [str(sid) for sid in selected_site_ids]
            if selected_camp:
                return [c.upper() for c in selected_camp]
            if selected_format:
                return [selected_format.upper()]
            return ["all"]                       # default view

        # 4️⃣  Render each tab
        for idx, metric_name in enumerate(metric_names):
            metric_id = id_lookup[metric_name]
            with tabs[idx]:
                st.caption(f"Metric ID {metric_id}")
                group_names = _active_group_names()

                with st.spinner("Loading data…"):
                    df = query_facts(
                        session=db,
                        metric_id=metric_id,
                        group_names=group_names,
                        period_level=selected_period_level          
                    )

                if df.empty:
                    st.info("No data for this selection.")
                    continue

                df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")

                # Sort by group and time to improve trace appearance
                df.sort_values(by=["group_name", "date"], inplace=True)
                if selected_site_ids:
                    df["group_name"] = df["group_name"].map({str(sid): id_to_name[sid] for sid in selected_site_ids if sid in id_to_name})
                fig = px.line(
                    df,
                    x="date",
                    y="value",
                    color="group_name",
                    markers=True,
                    labels={
                        "date": selected_period_label,
                        "value": "Metric Value",
                        "group_name": "Group"
                    },
                    title=f"{metric_name} Over Time by {selected_period_label}"
                )

                fig.update_traces(
                    mode="lines+markers",
                    hovertemplate=(
                        "<b>Group:</b> %{customdata[0]}<br>" +
                        "<b>Date:</b> %{x|%Y-%m-%d}<br>" +
                        "<b>Value:</b> %{y:.2f}<extra></extra>"
                    ),
                    customdata=df[["group_name"]]
                )

                # Layout tweaks for readability and style
                fig.update_layout(
                    height=450,
                    title_font_size=20,
                    legend_title="Selected Group",
                    xaxis=dict(
                        showgrid=False,
                        title=f"{selected_period_label}",
                        rangeselector=dict(
                            buttons=list([
                                dict(count=1, label="1m", step="month", stepmode="backward"),
                                dict(count=6, label="6m", step="month", stepmode="backward"),
                                dict(step="all")
                            ])
                        ),
                        rangeslider=dict(visible=True),
                        type="date"
                    ),
                    yaxis=dict(
                        showgrid=True,
                        zeroline=False,
                        title="Metric Value"
                    )
                )

                st.plotly_chart(fig, use_container_width=True)

