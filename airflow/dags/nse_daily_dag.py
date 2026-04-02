import sys
sys.path.insert(0, "/opt/airflow")

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from ingestion.fetch_nse import fetch_incremental
from ingestion.load_postgres import load_prices

DBT_DIR = "/opt/airflow/dbt_nse"


def run_daily_ingest():
    df = fetch_incremental(days=2)
    loaded = load_prices(df)
    print(f"Loaded {loaded} rows")


with DAG(
    dag_id="nse_daily_load",
    description="Daily incremental load of NSE prices at 18:00 EAT (Mon-Fri)",
    start_date=datetime(2024, 1, 1),
    schedule="0 15 * * 1-5",
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["nse", "daily"],
) as dag:

    ingest = PythonOperator(
        task_id="ingest_daily_prices",
        python_callable=run_daily_ingest,
    )

    dbt_run = BashOperator(
        task_id="dbt_run_models",
        bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir . --target prod",
    )

    dbt_test = BashOperator(
        task_id="dbt_test_models",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir . --target prod",
    )

    ingest >> dbt_run >> dbt_test
