import os

import helpers.sidebar
import pandas as pd
import plotly.express as px
import streamlit as st

from src.scripts.data_warehouse.access import getCamps, getMetricByID, getMetricFromCategory, getSites, query_facts
from src.scripts.data_warehouse.models.warehouse import get_db


current_dir = os.path.dirname(os.path.abspath(__file__))
svg_path_250 = os.path.join(
    current_dir, "..", "helpers", "static", "logo-250-years.svg")

if os.path.exists(svg_path_250):
    _page_icon_path = svg_path_250
else:
    _page_icon_path = ":material/emoticon:"
    st.warning(f"Logo not found: {os.path.basename(svg_path_250)}")

st.set_page_config(page_title="Customer Survey",
                   page_icon=_page_icon_path, layout="wide")



helpers.sidebar.show()
st.header("Customer Survey")

snapshot_container = st.container()  



data_visualization, menu_selection = st.columns([2, 1])

with menu_selection:
    st.subheader("Target Selection")

    db = next(get_db())


    all_sites_data = getSites(db)
    unique_store_formats = ["MAIN STORE", "MARINE MART"]
    store_formats = [fmt.upper() for fmt in unique_store_formats]
    selected_format = st.pills("Select Store Format", [
                               fmt.title() for fmt in store_formats], default=None)


    filtered_sites_data = [
        site
        for site in all_sites_data
        if (not selected_format or (site.store_format and site.store_format == selected_format.upper()))
    ]


    camps_data = getCamps(db)
    camp_names = sorted(camp.name for camp in camps_data if camp.name)
    selected_camp = st.multiselect(
        "Select Camp(s)", [name.title() for name in camp_names], default=[])

    if selected_camp:
        filtered_sites_data = [
            site
            for site in filtered_sites_data
            if site.command_name and site.command_name.upper() in {c.upper() for c in selected_camp}
        ]


    id_to_name = {
        site.site_id: site.site_name.title() for site in filtered_sites_data if site.site_id and site.site_name
    }

    selected_site_ids = st.multiselect(
        "Select Site(s)",
        list(id_to_name.keys()),
        default=[],
        format_func=lambda sid: id_to_name[sid],
    )

    if selected_site_ids:
        filtered_sites_data = [
            site for site in filtered_sites_data if site.site_id in selected_site_ids]

    # -------- Period --------------------------------------------------------- #
    PERIOD_LEVELS = {"Daily": 1, "Monthly": 2, "Quarterly": 3, "Yearly": 4}
    selected_period_label = st.selectbox(
        "Select Period", list(PERIOD_LEVELS.keys()), 0)
    selected_period_level = PERIOD_LEVELS[selected_period_label]

    st.button("Submit")



def _active_group_names() -> list[str]:
    if selected_site_ids:
        return [str(sid) for sid in selected_site_ids]
    if selected_camp:
        return [c.upper() for c in selected_camp]
    if selected_format:
        return [selected_format.upper()]
    return ["all"]




with data_visualization:
    st.subheader("Performance Metrics")
    db = next(get_db())

    retail_metric_ids = getMetricFromCategory(db, category=["Customer Survey"])

    metric_names, desc_lookup = [], {}
    for m_id in retail_metric_ids:
        metric = getMetricByID(db, m_id)
        if metric:
            metric_names.append(metric["metric_name"].title())
            desc_lookup[metric["id"]] = metric.get("metric_desc", "")
        else:
            metric_names.append(f"Metric ID {m_id}")
            desc_lookup[m_id] = "Description unavailable"

    tabs = st.tabs(metric_names)
    id_lookup = dict(zip(metric_names, retail_metric_ids))

    for idx, metric_name in enumerate(metric_names):
        metric_id = id_lookup[metric_name]
        with tabs[idx]:
            st.caption(desc_lookup.get(metric_id, ""))

            with st.spinner("Loading data…"):
                df = query_facts(
                    session=db,
                    metric_id=metric_id,
                    group_names=_active_group_names(),
                    period_level=selected_period_level,
                )

            if df.empty:
                st.info("No data for this selection.")
                continue


            df["date"] = pd.to_datetime(df["date"])
            df.sort_values(["group_name", "date"], inplace=True)
            if selected_site_ids:
                df["group_name"] = df["group_name"].map(
                    {str(sid): id_to_name[sid] for sid in selected_site_ids})

            fig = px.line(
                df,
                x="date",
                y="value",
                color="group_name",
                markers=True,
                labels={"date": selected_period_label,
                        "value": "Metric Value", "group_name": "Group"},
                title=f"{metric_name} Over Time ({selected_period_label})",
            )
            fig.update_traces(
                mode="lines+markers",
                hovertemplate="<b>Group:</b> %{customdata[0]}<br>"
                "<b>Date:</b> %{x|%Y-%m-%d}<br>"
                "<b>Value:</b> %{y:.2f}<extra></extra>",
                customdata=df[["group_name"]],
            )
            fig.update_layout(height=450)
            st.plotly_chart(fig, use_container_width=True)



with snapshot_container:
    st.divider()
    st.subheader("Overall Customer-Survey Snapshot (normalised %)")

    with st.spinner("Fetching data…"):
        group_names = _active_group_names()
        all_metrics_df = query_facts(
            session=db,
            metric_ids=retail_metric_ids,
            group_names=group_names,
            period_level=selected_period_level,
        )

    if all_metrics_df.empty:
        st.info("No data for the current selection.")
    else:
        all_metrics_df["date"] = pd.to_datetime(all_metrics_df["date"])
        all_metrics_df = all_metrics_df.sort_values("date", ascending=False).drop_duplicates(
            subset=["metric_id", "group_name"]
        )


        if selected_site_ids:
            all_metrics_df["group_name"] = all_metrics_df["group_name"].map(
                {str(sid): id_to_name[sid] for sid in selected_site_ids}
            )


        id_to_metric = {m: getMetricByID(
            db, m)["metric_name"].title() for m in retail_metric_ids}
        all_metrics_df["metric_name"] = all_metrics_df["metric_id"].map(
            id_to_metric)


        def _normalise(row):
            return row["value"] * 100 if row["metric_id"] == 7 else (row["value"] / 5) * 100

        all_metrics_df["value_pct"] = all_metrics_df.apply(
            _normalise, axis=1).round(1)


        heat = all_metrics_df.pivot(
            index="group_name", columns="metric_name", values="value_pct").sort_index()

        fig_summary = px.imshow(
            heat,
            aspect="auto",
            color_continuous_scale="Blues",
            text_auto=".1f",
            labels=dict(x="Metric", y="Group", color="Score (%)"),
            title="Normalised Mean Scores (0 – 100 %)",
        )


        raw_lookup = all_metrics_df.set_index(
            ["group_name", "metric_name"])["value"].round(2)
        fig_summary.update_traces(
            hovertemplate="<b>%{y}</b><br>"
            "<b>%{x}</b><br>"
            "Normalised: %{z:.1f}%<br>"
            "Raw value: %{customdata:.2f}<extra></extra>",
            customdata=[[raw_lookup[(g, m)] for m in heat.columns]
                        for g in heat.index],
        )
        fig_summary.update_xaxes(side="top")
        fig_summary.update_layout(height=400)

        st.plotly_chart(fig_summary, use_container_width=True)
