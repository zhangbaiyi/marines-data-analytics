import base64
import os

import streamlit as st

# Define default font sizes (if not defined elsewhere in your app)
title_font_size = "1.2em"
caption_font_size = "0.9em"


def show() -> None:
    """Displays the sidebar content including two logos side-by-side."""
    with st.sidebar:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        svg_path_250 = os.path.join(
            current_dir, "static", "logo-250-years.svg")
        svg_path_main = os.path.join(
            current_dir, "static", "logo-mccs-white.svg")

        # --- Display the first image in the first column ---
        if os.path.exists(svg_path_250):
            st.logo(
                image=svg_path_250,
                size="large",
            )
        else:
            st.warning(f"Logo not found: {os.path.basename(svg_path_250)}")

        st.header("MDAHub")
        st.subheader(
            "lorem ipsum dolor sit amet lorem ipsum dolor sit amet lorem ipsum dolor sit amet lorem ipsum dolor sit amet lorem ipsum dolor sit amet"
        )  # Placeholder for a subheader

        # --- Rest of the sidebar ---
        st.markdown("---")  # Optional separator
        reload_button = st.button("↪︎ Reload Page")
        if reload_button:
            # Consider clearing only specific keys if needed, otherwise clear() is fine
            st.session_state.clear()
            st.rerun()
