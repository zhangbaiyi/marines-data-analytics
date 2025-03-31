import helpers.sidebar
import streamlit as st

st.set_page_config(page_title="Retail",
                   page_icon=":material/storefront:", layout="wide")


helpers.sidebar.show()
# Display a toast notification
st.toast("Retail", icon=":material/storefront:")
st.header("retail")
st.write("placeholder")
