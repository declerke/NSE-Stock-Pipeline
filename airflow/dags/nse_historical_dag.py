import sys
sys.path.insert(0, "/opt/airflow")

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from ingestion.seed_historical import generate_all
from ingestion.load_postgres import load_prices

DBT_DIR = "/opt/airflow/dbt_nse"


def run_historical_ingest():
    df = generate_all()
    loaded = load_prices(df)
    print(f"Loaded {loaded} rows across {df['ticker'].nunique()} tickers ({df['trade_date'].min()} to {df['trade_date'].max()})")


with DAG(
    dag_id="nse_historical_backfill",
    description="One-time historical backfill of NSE OHLCV prices (2019 onwards)",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["nse", "historical", "backfill"],
) as dag:

    ingest = PythonOperator(
        task_id="ingest_historical_prices",
        python_callable=run_historical_ingest,
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed_tickers",
        bash_command=f"cd {DBT_DIR} && dbt seed --profiles-dir . --target prod",
    )

    dbt_run = BashOperator(
        task_id="dbt_run_models",
        bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir . --target prod",
    )

    dbt_test = BashOperator(
        task_id="dbt_test_models",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir . --target prod",
    )

    ingest >> dbt_seed >> dbt_run >> dbt_test
