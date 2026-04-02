import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

COLS = ["ticker", "trade_date", "open", "high", "low", "close", "volume"]


def get_engine():
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    db = os.getenv("POSTGRES_DB", "nse_db")
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")


def load_prices(df: pd.DataFrame) -> int:
    if df.empty:
        return 0

    engine = get_engine()

    with engine.begin() as conn:
        df[COLS].to_sql(
            "nse_prices_staging",
            conn,
            schema="raw",
            if_exists="replace",
            index=False,
            method="multi",
        )
        conn.execute(text("""
            INSERT INTO raw.nse_prices (ticker, trade_date, open, high, low, close, volume)
            SELECT ticker, trade_date, open, high, low, close, volume
            FROM raw.nse_prices_staging
            ON CONFLICT (ticker, trade_date) DO UPDATE SET
                open      = EXCLUDED.open,
                high      = EXCLUDED.high,
                low       = EXCLUDED.low,
                close     = EXCLUDED.close,
                volume    = EXCLUDED.volume,
                loaded_at = NOW()
        """))
        conn.execute(text("DROP TABLE IF EXISTS raw.nse_prices_staging"))

    return len(df)
