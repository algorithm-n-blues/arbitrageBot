import streamlit as st
import pandas as pd
from datetime import datetime

# Simulated DataFrame (replace with real-time data)
@st.cache_data
def get_initial_pyth_data():
    return pd.DataFrame({
        "Price Feed": ["1INCH/USD", "AAVE/USD", "ADA/USD"],
        "Asset Type": ["Crypto", "Crypto", "Crypto"],
        "Last Updated": ["<2s ago", "<2s ago", "<2s ago"],
        "Price": [0.405811, 333.472, 0.879105],  # Numeric data
        "Confidence": [0.00061, 0.467, 0.00092],  # Numeric data
        "1H": ["▼ 0.59%", "▲ 0.30%", "▼ 0.92%"],
        "24H": ["▲ 3.67%", "▲ 2.17%", "▲ 0.27%"],
        "7D": ["▲ 3.55%", "▲ 7.49%", "▼ 4.92%"],
    })

# Real-time updating mechanism
def update_data():
    # Replace with logic to update the Pyth price data
    updated_data = get_initial_pyth_data()  # Placeholder for actual listener output
    return updated_data

# Display dashboard
def display_dashboard():
    st.title("Pyth Network Price Feeds")
    st.subheader("Real-Time Price Updates")

    # Fetch updated data
    pyth_data = update_data()

    # Render the DataFrame in a styled table
    st.write("Price Feeds Table (Real-Time)")
    st.dataframe(
        pyth_data.style.format({
            "Price": "${:.6f}",
            "Confidence": "±${:.6f}"
        })
    )

# Call the dashboard
display_dashboard()