import os
import helpers.sidebar
import streamlit as st

current_dir = os.path.dirname(os.path.abspath(__file__))
svg_path_250 = os.path.join(
    current_dir, "..", "helpers", "static", "logo-250-years.svg")
svg_path_main = os.path.join(
    current_dir, "..", "helpers", "static", "logo-mccs-white.svg")

# --- Display the first image in the first column ---
if os.path.exists(svg_path_250):
    _page_icon_path = svg_path_250
else:
    _page_icon_path = ":material/alternate_email:"
    st.warning(f"Logo not found: {os.path.basename(svg_path_250)}")

st.set_page_config(page_title="Email",
                   page_icon=_page_icon_path, layout="wide")

if __name__ == "__main__":
    helpers.sidebar.show()
    # Display a toast notification
    st.toast("Email", icon=":material/alternate_email:")
    st.header("email")
    st.write("placeholder")


