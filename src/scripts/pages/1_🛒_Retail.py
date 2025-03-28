import streamlit as st
import helpers.sidebar

st.set_page_config(
    page_title="Retail",
    page_icon=":material/storefront:",
    layout="wide"
)


helpers.sidebar.show()
st.toast("Retail", icon=":material/storefront:")  # Display a toast notification
st.header("retail")
st.write("placeholder")

