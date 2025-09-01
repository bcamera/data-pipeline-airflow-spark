from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import yfinance as yf
import pandas as pd
import boto3

def extract_finance_data():
    tickers = ["PETR4.SA", "VALE3.SA", "ITUB4.SA", "^BVSP", "BTC-USD"]
    data = {}
    for ticker in tickers:
        df = yf.download(ticker, period="5y", interval="1d")
        df.reset_index(inplace=True)
        data[ticker] = df
        df.to_csv(f"/tmp/{ticker.replace('^','')}.csv", index=False)

def load_to_s3():
    s3 = boto3.client(
        's3',
        endpoint_url="http://minio:9000",
        aws_access_key_id="minio",
        aws_secret_access_key="minio123"
    )
    tickers = ["PETR4.SA", "VALE3.SA", "ITUB4.SA", "^BVSP", "BTC-USD"]
    for ticker in tickers:
        file = f"/tmp/{ticker.replace('^','')}.csv"
        s3.upload_file(file, "raw", f"finance/{ticker.replace('^','')}.csv")

def transform_finance_data():
    tickers = ["PETR4.SA", "VALE3.SA", "ITUB4.SA", "^BVSP", "BTC-USD"]
    s3 = boto3.client(
        's3',
        endpoint_url="http://minio:9000",
        aws_access_key_id="minio",
        aws_secret_access_key="minio123"
    )
    for ticker in tickers:
        df = pd.read_csv(f"/tmp/{ticker.replace('^','')}.csv")
        df["Return"] = df["Close"].pct_change()
        df["MA7"] = df["Close"].rolling(window=7).mean()
        df["MA30"] = df["Close"].rolling(window=30).mean()
        df["Volatility"] = df["Return"].rolling(window=30).std()

        file_curated = f"/tmp/{ticker.replace('^','')}_curated.csv"
        df.to_csv(file_curated, index=False)
        s3.upload_file(file_curated, "curated", f"finance/{ticker.replace('^','')}_curated.csv")

with DAG("etl_pipeline_finance", start_date=datetime(2025,1,1),
         schedule_interval="@daily", catchup=False) as dag:

    extract = PythonOperator(task_id="extract", python_callable=extract_finance_data)
    load = PythonOperator(task_id="load", python_callable=load_to_s3)
    transform = PythonOperator(task_id="transform", python_callable=transform_finance_data)

    extract >> load >> transform
