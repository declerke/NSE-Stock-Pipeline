import os
import streamlit as st
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()


@st.cache_resource
def get_engine():
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    db = os.getenv("POSTGRES_DB", "nse_db")
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")
