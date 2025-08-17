"""
Production CoinGecko ETL Pipeline
Extracts top 50 cryptocurrency market data every 15 minutes
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.filesystem import FileSensor
from airflow.models import Variable
import requests
import json
import logging
import os
from pathlib import Path

COINGECKO_API_URL = "https://api.coingecko.com/api/v3"
POSTGRES_CONN_ID = "postgres_default"
RAW_DATA_PATH = "/opt/airflow/data/raw"
PROCESSED_DATA_PATH = "/opt/airflow/data/processed"
CRYPTO_API_KEY = os.environ.get('CRYPTO_API_KEY')

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'catchup': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'coingecko_etl_pipeline',
    default_args=default_args,
    description='CoinGecko cryptocurrency market data ETL pipeline',
    schedule_interval=timedelta(minutes=15),
    max_active_runs=1,
    tags=['crypto', 'etl', 'coingecko']
)

def extract_market_data(**context) -> str:
    """
    Extract top 50 cryptocurrency market data from CoinGecko API
    """
    execution_date = context['execution_date']
    timestamp_str = execution_date.strftime('%Y%m%d_%H%M%S')
    
    logging.info(f"Starting extraction for execution date: {execution_date}")
    
    try:
        endpoint = f"{COINGECKO_API_URL}/coins/markets"
        params = {
            'vs_currency': 'usd',
            'order': 'market_cap_desc',
            'per_page': 50,
            'page': 1,
            'sparkline': False,
            'price_change_percentage': '1h, 24h, 7d, 30d'
        }
        headers = {
            "accept": "application/json",
            "x-cg-pro-api-key": CRYPTO_API_KEY   
        }

        response = requests.get(endpoint, params=params, headers=headers, timeout=30)
        response.raise_for_status()

        market_data = response.json()

        enriched_data = {
            'extraction_timestamp': execution_date.isoformat(),
            'api_endpoint': endpoint,
            'record_count': len(market_data),
            'data': market_data
        }

        raw_path = Path(RAW_DATA_PATH)
        raw_path.mkdir(parents=True, exist_ok=True)

        filename = f"coingecko_market_data_{timestamp_str}.json"
        filepath = raw_path / filename

        with open(filepath, 'w') as f:
            json.dump(enriched_data, f, indent=2)

        logging.info(f"Data extracted successfully, {len(market_data)}")

        return str(filepath)
    
    except requests.RequestException as e:
        logging.error(f"Error during API request: {e}")
        raise
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON response: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error during extraction: {e}")
        raise

extract_data = PythonOperator(
    task_id='extract_market_data',
    python_callable=extract_market_data,
    dag=dag
)
