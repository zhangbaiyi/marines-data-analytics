import helpers.sidebar
import streamlit as st

st.set_page_config(page_title="socialmedia",
                   page_icon=":material/public:", layout="wide")


helpers.sidebar.show()
# Display a toast notification
st.toast("Social Media", icon=":material/public:")
st.header("socialmedia")
st.write("placeholder")
