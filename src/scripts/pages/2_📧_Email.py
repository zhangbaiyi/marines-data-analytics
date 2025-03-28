import streamlit as st
import helpers.sidebar

st.set_page_config(
    page_title="Email",
    page_icon=":material/alternate_email:",
    layout="wide"
)


helpers.sidebar.show()
st.toast("Email", icon=":material/alternate_email:")  # Display a toast notification
st.header("email")
st.write("placeholder")

