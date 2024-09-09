from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import yfinance as yf
import requests
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from elasticsearch import Elasticsearch
import json
import os

# Replace with your Alpha Vantage API key
ALPHA_VANTAGE_API_KEY = '07M6PDB5S6KZG75Q'

# Elasticsearch configuration
ELASTICSEARCH_HOST = 'localhost'
ELASTICSEARCH_PORT = 9200
ELASTICSEARCH_USER = 'elastic'
ELASTICSEARCH_PASSWORD = 'WVhR38vYK04uI=updDPE'

# List of stock tickers
TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "FB", "TSLA", "BRK-B", "JPM", "JNJ", "V", 
           "PG", "UNH", "NVDA", "HD", "DIS", "MA", "PYPL", "VZ", "ADBE", "NFLX", 
           "KO", "INTC", "MRK", "PFE", "T", "PEP", "CSCO", "XOM", "ABT", "CRM", 
           "MCD", "NKE", "WMT", "BMY", "NEE", "MDT", "LLY", "CMCSA", "COST", 
           "AVGO", "HON", "ACN", "ORCL", "TXN", "AMGN", "QCOM", "PM", "LOW", "IBM"]

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'stock_data_pipeline',
    default_args=default_args,
    description='A simple stock data pipeline',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

def fetch_yahoo_finance_data(**kwargs):
    tickers = kwargs['tickers']
    data_dir = kwargs['data_dir']
    all_data = []

    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    for ticker in tickers:
        data = yf.download(ticker, start="2020-01-01", end="2023-12-31")
        data['Ticker'] = ticker
        all_data.append(data)
    
    aggregated_data = pd.concat(all_data)
    aggregated_data.to_csv(f'{data_dir}/yahoo_all_tickers_data.csv')
    print(f"Aggregated Yahoo Finance data saved to {data_dir}/yahoo_all_tickers_data.csv")

def fetch_alpha_vantage_data(**kwargs):
    tickers = kwargs['tickers']
    api_key = kwargs['api_key']
    data_dir = kwargs['data_dir']
    all_data = []

    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    for ticker in tickers:
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={ticker}&apikey={api_key}'
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            daily_data = pd.DataFrame.from_dict(data['Time Series (Daily)'], orient='index', dtype=float)
            daily_data.columns = ['1. open', '2. high', '3. low', '4. close', '5. volume']
            daily_data['Ticker'] = ticker
            all_data.append(daily_data)

    aggregated_data = pd.concat(all_data)
    aggregated_data.to_csv(f'{data_dir}/alpha_all_tickers_data.csv')
    print(f"Aggregated Alpha Vantage data saved to {data_dir}/alpha_all_tickers_data.csv")

def process_data(**kwargs):
    data_dir = kwargs['data_dir']
    yahoo_file_path = f'{data_dir}/yahoo_all_tickers_data.csv'
    alpha_file_path = f'{data_dir}/alpha_all_tickers_data.csv'

    # Load the CSV files
    yahoo_df = pd.read_csv(yahoo_file_path)
    alpha_df = pd.read_csv(alpha_file_path)

    # Rename the columns Unnamed: 0 to Date
    alpha_df.rename(columns={'Unnamed: 0': 'Date'}, inplace=True)

    if 'Date' in yahoo_df.columns:
        yahoo_df['Date'] = pd.to_datetime(yahoo_df['Date'])
    if 'Date' in alpha_df.columns:
        alpha_df['Date'] = pd.to_datetime(alpha_df['Date'])

    # Handling missing values
    yahoo_df.fillna(method='ffill', inplace=True)
    alpha_df.fillna(method='ffill', inplace=True)

    # Feature Engineering: Example - Adding daily return
    if 'Close' in yahoo_df.columns:
        yahoo_df['Daily_Return'] = yahoo_df['Close'].pct_change()
    if '4. close' in alpha_df.columns:
        alpha_df['Daily_Return'] = alpha_df['4. close'].pct_change()

    # Normalization/Scaling: Scale the 'Close' prices using MinMaxScaler
    scaler = MinMaxScaler()
    if 'Close' in yahoo_df.columns:
        yahoo_df['Close_Scaled'] = scaler.fit_transform(yahoo_df[['Close']])
    if '4. close' in alpha_df.columns:
        alpha_df['Close_Scaled'] = scaler.fit_transform(alpha_df[['4. close']])

    # Removing duplicates
    yahoo_df.drop_duplicates(inplace=True)
    alpha_df.drop_duplicates(inplace=True)

    # Renaming columns for consistency
    yahoo_df.rename(columns={
        'Date': 'date',
        'Open': 'open_yahoo',
        'High': 'high_yahoo',
        'Low': 'low_yahoo',
        'Close': 'close_yahoo',
        'Adj Close': 'adj_close_yahoo',
        'Volume': 'volume_yahoo'
    }, inplace=True)

    alpha_df.rename(columns={
        'date': 'date',
        '1. open': 'open_alpha',
        '2. high': 'high_alpha',
        '3. low': 'low_alpha',
        '4. close': 'close_alpha',
        '5. volume': 'volume_alpha'
    }, inplace=True)

    # Save the preprocessed data to new CSV files
    preprocessed_yahoo_file_path = f'{data_dir}/preprocessed_yahoo_all_tickers_data.csv'
    preprocessed_alpha_file_path = f'{data_dir}/preprocessed_alpha_all_tickers_data.csv'
    yahoo_df.to_csv(preprocessed_yahoo_file_path, index=False)
    alpha_df.to_csv(preprocessed_alpha_file_path, index=False)

    # Load the preprocessed CSV files
    yahoo_df = pd.read_csv(preprocessed_yahoo_file_path)
    alpha_df = pd.read_csv(preprocessed_alpha_file_path)

    # Add a 'source' column to differentiate data sources
    yahoo_df['source'] = 'yahoo'
    alpha_df['source'] = 'alpha'

    # Rename columns for consistency
    yahoo_df.rename(columns={
        'Date': 'date',
        'open_yahoo': 'open',
        'high_yahoo': 'high',
        'low_yahoo': 'low',
        'close_yahoo': 'close',
        'adj_close_yahoo': 'adj_close',
        'volume_yahoo': 'volume'
    }, inplace=True)

    alpha_df.rename(columns={
        'Date': 'date',
        'open_alpha': 'open',
        'high_alpha': 'high',
        'low_alpha': 'low',
        'close_alpha': 'close',
        'volume_alpha': 'volume'
    }, inplace=True)

    # Concatenate the dataframes
    combined_df = pd.concat([yahoo_df, alpha_df], ignore_index=True)

    # Calculate Z-score for the closing prices
    combined_df['close_zscore'] = (combined_df['close'] - combined_df['close'].mean()) / combined_df['close'].std()

    # Flag anomalies where Z-score is greater than 3 or less than -3
    combined_df['anomaly'] = combined_df['close_zscore'].apply(lambda x: 'Yes' if abs(x) > 3 else 'No')

    # Save the combined data to a single CSV file
    combined_file_path = f'{data_dir}/combined_yahoo_alpha_data.csv'
    combined_df.to_csv(combined_file_path, index=False)

def index_data_to_elasticsearch(**kwargs):
    data_dir = kwargs['data_dir']
    es = Elasticsearch(
        hosts=[{'host': ELASTICSEARCH_HOST, 'port': ELASTICSEARCH_PORT}],
        http_auth=(ELASTICSEARCH_USER, ELASTICSEARCH_PASSWORD),
        scheme="https",
        verify_certs=False,
    )
    df = pd.read_csv(f'{data_dir}/combined_yahoo_alpha_data.csv')
    df = df.replace({np.nan: None})
    for i, row in df.iterrows():
        doc = row.to_dict()
        es.index(index='stock_anomalies', body=doc)

fetch_yahoo_task = PythonOperator(
    task_id='fetch_yahoo_finance_data',
    python_callable=fetch_yahoo_finance_data,
    op_kwargs={'tickers': TICKERS, 'data_dir': '/Users/abdullahmac/airflow/dags/stock_data'},
    dag=dag,
)

fetch_alpha_task = PythonOperator(
    task_id='fetch_alpha_vantage_data',
    python_callable=fetch_alpha_vantage_data,
    op_kwargs={'tickers': TICKERS, 'api_key': ALPHA_VANTAGE_API_KEY, 'data_dir': '/Users/abdullahmac/airflow/dags/stock_data'},
    dag=dag,
)

process_data_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    op_kwargs={'data_dir': '/Users/abdullahmac/airflow/dags/stock_data'},
    dag=dag,
)

index_data_task = PythonOperator(
    task_id='index_data_to_elasticsearch',
    python_callable=index_data_to_elasticsearch,
    op_kwargs={'data_dir': '/Users/abdullahmac/airflow/dags/stock_data'},
    dag=dag,
)

fetch_yahoo_task >> fetch_alpha_task >> process_data_task >> index_data_task
