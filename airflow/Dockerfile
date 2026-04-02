FROM apache/airflow:2.8.1-python3.11
USER root
RUN apt-get update && apt-get install -y --no-install-recommends gcc && rm -rf /var/lib/apt/lists/*
USER airflow
RUN pip install --no-cache-dir \
    yfinance==0.2.37 \
    psycopg2-binary==2.9.9 \
    dbt-postgres==1.7.14 \
    python-dotenv==1.0.1
