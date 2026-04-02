import pandas as pd
import streamlit as st
from utils import get_engine

st.set_page_config(
    page_title="NSE Stock Pipeline",
    page_icon="📈",
    layout="wide",
)

st.title("NSE Stock Pipeline")
st.markdown("Real-time analytics for Nairobi Securities Exchange listed equities.")
st.markdown("---")

engine = get_engine()


@st.cache_data(ttl=3600)
def get_summary():
    try:
        df = pd.read_sql(
            "SELECT ticker, MAX(trade_date) AS last_date, COUNT(*) AS rows "
            "FROM raw.nse_prices GROUP BY ticker ORDER BY ticker",
            engine,
        )
        return df
    except Exception:
        return pd.DataFrame()


summary = get_summary()

col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Exchange", "Nairobi Securities Exchange")
with col2:
    st.metric("Tickers Tracked", len(summary) if not summary.empty else 0)
with col3:
    last = summary["last_date"].max() if not summary.empty else "N/A"
    st.metric("Last Loaded", str(last))

st.markdown("---")

if summary.empty:
    st.warning("No data loaded yet. Trigger the `nse_historical_backfill` DAG in Airflow first.")
else:
    st.subheader("Coverage by Ticker")
    st.dataframe(summary, use_container_width=True)

st.markdown("Use the sidebar to navigate between analysis pages.")
