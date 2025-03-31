import helpers.sidebar
import streamlit as st

st.set_page_config(page_title="Email",
                   page_icon=":material/alternate_email:", layout="wide")


helpers.sidebar.show()
# Display a toast notification
st.toast("Email", icon=":material/alternate_email:")
st.header("email")
st.write("placeholder")
