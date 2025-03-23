import streamlit as st

def main():
    st.title("Prediction Model Application")

    # Example interaction (replace with your actual prediction logic)
    input_value = st.number_input("Enter a numeric value:", min_value=0, max_value=100)

    if st.button("Predict"):
        # Call your model prediction function here
        st.write(f"Prediction: Hello world")