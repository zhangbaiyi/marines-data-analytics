
import streamlit as st
import helpers.sidebar



st.set_page_config(
page_title="About",
page_icon=":material/info:",
layout="wide"

)

helpers.sidebar.show()
st.toast("About", icon=":material/info:")
st.markdown("Lorem ipsum ")


