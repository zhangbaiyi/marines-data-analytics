from src.scripts.data_warehouse.models.warehouse import get_db
from src.scripts.data_warehouse.access import getCamps, getMetricByID, getMetricFromCategory, getSites, query_facts
import streamlit as st
import helpers.sidebar
from typing import List
import os

import altair as alt
import pandas as pd
import plotly.express as px

alt.data_transformers.disable_max_rows()


current_dir = os.path.dirname(os.path.abspath(__file__))
svg_path_250 = os.path.join(
    current_dir, "..", "helpers", "static", "logo-250-years.svg")
svg_path_main = os.path.join(
    current_dir, "..", "helpers", "static", "logo-mccs-white.svg")


if os.path.exists(svg_path_250):
    _page_icon_path = svg_path_250
else:
    _page_icon_path = ":material/storefront:"
    st.warning(f"Logo not found: {os.path.basename(svg_path_250)}")

st.set_page_config(page_title="Retail Insights",
                   page_icon=_page_icon_path, layout="wide")

if __name__ == "__main__":
    helpers.sidebar.show()
    st.header("Retail Insights")


with st.container():
    st.subheader("Camp-level Metric Map")

    # ---------- database + camp locations ----------
    db         = next(get_db())
    camps_data = getCamps(db)

    camp_df = pd.DataFrame(
        {
            "group_name": [c.name.upper() for c in camps_data],
            "Camp"      : [c.name.title() for c in camps_data],
            "lat"       : [c.lat for c in camps_data],
            "lon"       : [c.long for c in camps_data],
        }
    )

    # ---------- metric selector ----------
    retail_metric_ids = getMetricFromCategory(db, category=["Retail"])
    id_to_metric      = {
        mid: getMetricByID(db, mid)["metric_name"].title()
        for mid in retail_metric_ids
    }

    metric_choice_name = st.selectbox(
        "Select metric",
        options=list(id_to_metric.values()),
        index=0,                      # default shown to user
        key="metric_select",
    )
    metric_choice_id = next(k for k, v in id_to_metric.items()
                            if v == metric_choice_name)

    # ---------- pull facts for the chosen metric ----------
    group_names = camp_df["group_name"].tolist()
    map_df = query_facts(
        session      = db,
        metric_id    = metric_choice_id,   # <- now dynamic
        group_names  = group_names,
        period_level = 2,
    )

    if map_df.empty:
        st.info("No data for this metric.")
        st.stop()

    # ---------- tidy & filter by month ----------
    map_df["date"] = pd.to_datetime(map_df["date"])
    map_df         = map_df.merge(camp_df, on="group_name", how="inner")

    months_sorted = sorted(
        map_df["date"].dt.strftime("%b %Y").unique(),
        key=lambda x: pd.to_datetime(x, format="%b %Y"),
    )
    month_choice = st.selectbox(
        "Select month",
        months_sorted,
        index=len(months_sorted) - 1,
        key="month_select",
    )
    sel_month_df = map_df[
        map_df["date"].dt.to_period("M")
        == pd.to_datetime(month_choice).to_period("M")
    ]

    # ---------- plot ----------
    fig_map = px.scatter_mapbox(
        sel_month_df,
        lat       = "lat",
        lon       = "lon",
        hover_name= "Camp",
        hover_data= {"value": ":,.0f"},
        color     = "value",
        size      = "value",
        size_max  = 100,
        zoom      = 5,
        height    = 500,
    )
    fig_map.update_layout(
        mapbox_style="open-street-map",
        margin=dict(l=0, r=0, t=0, b=0),
        coloraxis_colorbar=dict(title="Value"),
        showlegend=True,
    )
    st.plotly_chart(fig_map, use_container_width=True)

    data_visualization, menu_selection = st.columns([2, 1])

  
    with menu_selection:
        st.subheader("Target Selection")

        db = next(get_db())

        all_sites_data = getSites(db)
        unique_store_formats = ["MAIN STORE", "MARINE MART"]
        store_formats = [fmt.upper() for fmt in unique_store_formats if fmt]
        selected_format = st.pills("Select Store Format", [
                                   fmt.title() for fmt in store_formats], default=None)

        if selected_format:
            filtered_sites_data = [
                site for site in all_sites_data if site.store_format and site.store_format == selected_format.upper()
            ]
        else:
            filtered_sites_data = all_sites_data

        camps_data = getCamps(db)
        camp_names = sorted(
            [camp.name for camp in camps_data if camp.name]) if camps_data else []
        selected_camp = st.multiselect(
            "Select Camp(s)", [name.title() for name in camp_names], default=[])

        if selected_camp:
            filtered_sites_data = [
                site
                for site in filtered_sites_data
                if site.command_name and site.command_name.upper() in [sc.upper() for sc in selected_camp]
            ]

        if filtered_sites_data:
            id_to_name = {
                site.site_id: site.site_name.title() for site in filtered_sites_data if site.site_id and site.site_name
            }
            site_options = sorted(id_to_name.keys())

            selected_site_ids = st.multiselect(
                "Select Site(s)",
                site_options,
                default=[],
                format_func=lambda sid: id_to_name[sid],
            )
        else:
            selected_site_ids = []

        if selected_site_ids:
            filtered_sites_data = [
                site for site in filtered_sites_data if site.site_id in selected_site_ids]

        PERIOD_LEVELS = {"Daily": 1, "Monthly": 2, "Quarterly": 3, "Yearly": 4}
        selected_period_label = st.selectbox(
            "Select Period", options=list(PERIOD_LEVELS.keys()), index=1)
        selected_period_level = PERIOD_LEVELS[selected_period_label]

        st.button("Submit")

    with data_visualization:
        st.subheader("Performance Metrics")

        db = next(get_db())

        retail_metric_ids = getMetricFromCategory(db, category=["Retail"])

        metric_names: list[str] = []
        desc_lookup: dict[int, str] = {}

        for m_id in retail_metric_ids:
            metric = getMetricByID(db, m_id)
            if metric:
                metric_names.append(metric["metric_name"].title())
                print(metric)
                desc_lookup[metric["id"]] = metric.get("metric_desc", "")
            else:
                metric_names.append(f"Metric ID {m_id}")
                desc_lookup[m_id] = "Description unavailable"

        tabs = st.tabs(metric_names)
        id_lookup = dict(zip(metric_names, retail_metric_ids)
                         ) 

        def _active_group_names() -> list[str]:
            if selected_site_ids:
                return [str(sid) for sid in selected_site_ids]
            if selected_camp:
                return [c.upper() for c in selected_camp]
            if selected_format:
                return [selected_format.upper()]
            return ["all"]  # default view

        for idx, metric_name in enumerate(metric_names):
            metric_id = id_lookup[metric_name]
            with tabs[idx]:
                st.caption(desc_lookup.get(metric_id, ""))

                group_names = _active_group_names()

                with st.spinner("Loading data…"):
                    df = query_facts(
                        session=db,
                        metric_id=metric_id,
                        group_names=group_names,
                        period_level=selected_period_level,
                    )

                if df.empty:
                    st.info("No data for this selection.")
                    continue

                df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")

                df.sort_values(by=["group_name", "date"], inplace=True)
                if selected_site_ids:
                    df["group_name"] = df["group_name"].map(
                        {str(sid): id_to_name[sid]
                         for sid in selected_site_ids if sid in id_to_name}
                    )

                fig = px.line(
                    df,
                    x="date",
                    y="value",
                    color="group_name",
                    markers=True,
                    labels={
                        "date": selected_period_label,
                        "value": "Metric Value",
                        "group_name": "Group",
                    },
                    title=f"{metric_name} Over Time by {selected_period_label}",
                )

                fig.update_traces(
                    mode="lines+markers",
                    hovertemplate="<b>Group:</b> %{customdata[0]}<br><b>Date:</b> %{x|%Y-%m-%d}<br><b>Value:</b> %{y:.2f}<extra></extra>",
                    customdata=df[["group_name"]],
                )

                fig.update_layout(
                    height=450,
                    title_font_size=20,
                    legend_title="Selected Group",
                    xaxis=dict(
                        showgrid=False,
                        title=f"{selected_period_label}",
                        rangeselector=dict(
                            buttons=list(
                                [
                                    dict(count=1, label="1m",
                                         step="month", stepmode="backward"),
                                    dict(count=6, label="6m",
                                         step="month", stepmode="backward"),
                                    dict(step="all"),
                                ]
                            )
                        ),
                        rangeslider=dict(visible=True),
                        type="date",
                    ),
                    yaxis=dict(showgrid=True, zeroline=False,
                               title="Metric Value"),
                )

                st.plotly_chart(fig, use_container_width=True)
