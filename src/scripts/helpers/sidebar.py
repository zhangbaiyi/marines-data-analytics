import base64
import os
import streamlit as st


def show() -> None:

    
    with st.sidebar:
        current_dir = os.path.dirname(__file__)
        svg_path = os.path.join(current_dir, "static", "logo-250-years.svg")

        # Read and Base64-encode the SVG content
        with open(svg_path, "r", encoding="utf-8") as f:
            svg_content = f.read()
        svg_base64 = base64.b64encode(svg_content.encode("utf-8")).decode("utf-8")

        st.markdown(
            f"""
            <a href="/" style="color:black;text-decoration: none;">
                <div style="display:table;margin-top:1rem;margin-left:0%;">
                    <img src="data:image/svg+xml;base64,{svg_base64}" width="30" style="vertical-align:middle;">
                    <span style="vertical-align:middle;">MDAHub</span>
                    <br>
                    <span style="font-size: 0.8em">Your AI-powered personal finance assistant!</span>
                </div>
            </a>
            <br>
            """,
            unsafe_allow_html=True,
        )


        reload_button = st.button("↪︎ Reload Page")
        if reload_button:
            st.session_state.clear()
            st.rerun()
