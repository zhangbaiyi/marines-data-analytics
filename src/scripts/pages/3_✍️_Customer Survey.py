import streamlit as st
import helpers.sidebar

st.set_page_config(
    page_title="Customer Survey",
    page_icon=":material/emoticon:",
    layout="wide"
)


helpers.sidebar.show()
st.toast("Customer Survey", icon=":material/emoticon:")
st.header("custumersurvey")
st.write("placeholder")

