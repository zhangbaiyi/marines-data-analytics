import os

import altair as alt
import helpers.sidebar
import pandas as pd
import plotly.express as px
import streamlit as st

from src.scripts.data_warehouse.access import getCamps, getMetricByID, getMetricFromCategory, getSites, query_facts
from src.scripts.data_warehouse.models.warehouse import get_db

current_dir = os.path.dirname(os.path.abspath(__file__))
svg_path_250 = os.path.join(
    current_dir, "..", "helpers", "static", "logo-250-years.svg")
svg_path_main = os.path.join(
    current_dir, "..", "helpers", "static", "logo-mccs-white.svg")

if os.path.exists(svg_path_250):
    _page_icon_path = svg_path_250
else:
    _page_icon_path = ":material/alternate_email:"
    st.warning(f"Logo not found: {os.path.basename(svg_path_250)}")

st.set_page_config(page_title="Marketing Insights",
                   page_icon=_page_icon_path, layout="wide")

if __name__ == "__main__":
    helpers.sidebar.show()
    st.header("Marketing Insights")

    data_visualization, menu_selection = st.columns([2, 1])

    with menu_selection:
        st.subheader("Target Selection")

        db = next(get_db())

        PERIOD_LEVELS = {"Daily": 1, "Monthly": 2, "Quarterly": 3, "Yearly": 4}
        selected_period_label = st.selectbox(
            "Select Period", options=list(PERIOD_LEVELS.keys()), index=1
        )
        selected_period_level = PERIOD_LEVELS[selected_period_label]

        st.button("Submit")

    with data_visualization:
        st.subheader("Performance Metrics")

        db = next(get_db())

        retail_metric_ids = getMetricFromCategory(
            db, category=["Email & Social Media"])

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
        id_lookup = dict(zip(metric_names, retail_metric_ids))

        for idx, metric_name in enumerate(metric_names):
            metric_id = id_lookup[metric_name]
            with tabs[idx]:
                st.caption(desc_lookup.get(metric_id, ""))

                group_names = ["all"]

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
