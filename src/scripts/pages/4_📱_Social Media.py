import streamlit as st
import helpers.sidebar

st.set_page_config(
    page_title="socialmedia",
    page_icon=":material/public:",
    layout="wide"
)


helpers.sidebar.show()
st.toast("Social Media", icon=":material/public:")  # Display a toast notification
st.header("socialmedia")
st.write("placeholder")

