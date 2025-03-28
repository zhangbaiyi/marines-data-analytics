
import streamlit as st
import helpers.sidebar



st.set_page_config(
page_title="MDAHub",
page_icon="🎖️",
layout="wide"

)

helpers.sidebar.show()
st.toast("Welcome to MDAHub", icon="🎖️")
st.markdown("Welcome to the all-in-one data analytics solution for MCCS!")
st.write("Explore different kinds of data analytics features from the sidebar!")


