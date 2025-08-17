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

create_tables_sql = """
-- Raw data lake table
CREATE TABLE IF NOT EXISTS raw_market_data (
    id SERIAL PRIMARY KEY,
    extraction_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    api_endpoint TEXT NOT NULL,
    record_count INTEGER NOT NULL,
    raw_json_data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_extraction UNIQUE (extraction_timestamp)
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_raw_market_data_extraction_timestamp 
ON raw_market_data(extraction_timestamp);

CREATE INDEX IF NOT EXISTS idx_raw_market_data_created_at 
ON raw_market_data(created_at);

-- Curated market data table (for final processed data)
CREATE TABLE IF NOT EXISTS curated_market_data (
    id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    name TEXT NOT NULL,
    current_price DECIMAL(20,8),
    market_cap BIGINT,
    market_cap_rank INTEGER,
    fully_diluted_valuation BIGINT,
    total_volume DECIMAL(20,8),
    high_24h DECIMAL(20,8),
    low_24h DECIMAL(20,8),
    price_change_24h DECIMAL(20,8),
    price_change_percentage_24h DECIMAL(10,4),
    price_change_percentage_1h DECIMAL(10,4),
    price_change_percentage_7d DECIMAL(10,4),
    price_change_percentage_30d DECIMAL(10,4),
    market_cap_change_24h BIGINT,
    market_cap_change_percentage_24h DECIMAL(10,4),
    circulating_supply DECIMAL(20,8),
    total_supply DECIMAL(20,8),
    max_supply DECIMAL(20,8),
    ath DECIMAL(20,8),
    ath_change_percentage DECIMAL(10,4),
    ath_date TIMESTAMP WITH TIME ZONE,
    atl DECIMAL(20,8),
    atl_change_percentage DECIMAL(10,4),
    atl_date TIMESTAMP WITH TIME ZONE,
    last_updated TIMESTAMP WITH TIME ZONE,
    extraction_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    partition_date DATE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Partitioning for better performance (PostgreSQL 10+)
-- Note: In production, consider using native partitioning
CREATE INDEX IF NOT EXISTS idx_curated_market_data_partition_date 
ON curated_market_data(partition_date);

CREATE INDEX IF NOT EXISTS idx_curated_market_data_symbol 
ON curated_market_data(symbol);

CREATE INDEX IF NOT EXISTS idx_curated_market_data_market_cap_rank 
ON curated_market_data(market_cap_rank);
"""

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

def load_rawdata_to_postgres(**context):
    """
    Load raw JSON data to PostgreSQL data lake
    """
    filepath = context['task_instance'].xcom_pull(task_ids='extract_market_data')
    execution_date = context.get('data_interval_start') or context.get('logical_date')
    
    logging.info(f"Loading raw data from {filepath} to PostgreSQL")
    
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)

        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        insert_query = """
        INSERT INTO raw_market_data (
            extraction_timestamp,
            api_endpoint,
            record_count,
            raw_json_data,
            created_at
        ) VALUES (%s, %s, %s, %s, %s)
        """

        postgres_hook.run(
            insert_query,
            parameters=(
                data['extraction_timestamp'],
                data['api_endpoint'],
                data['record_count'],
                json.dumps(data['data']),
                execution_date
            )
        )

        logging.info(f"Raw data loaded successfully for {execution_date}")
        
        return {
            'extraction_timestamp': data['extraction_timestamp'],
            'record_count': data['record_count'],
            'filepath': filepath
        }
        
    except Exception as e:
        logging.error(f"Failed to load raw data to PostgreSQL: {e}")
        raise

def submit_spark_transformation(**context):
    """
    Submit PySpark job for data transformation
    """
    metadata = context['task_instance'].xcom_pull(task_ids='load_raw_to_postgres')
    execution_date = context['execution_date']
    
    logging.info("Preparing Spark transformation job")
    
    processed_path = Path(PROCESSED_DATA_PATH)
    processed_path.mkdir(parents=True, exist_ok=True)
    
    spark_params = {
        'extraction_timestamp': metadata['extraction_timestamp'],
        'execution_date': execution_date.isoformat(),
        'output_path': str(processed_path)
    }
    
    context['task_instance'].xcom_push(key='spark_params', value=spark_params)
    
    return spark_params

create_tables = PostgresOperator(
    task_id='create_tables',
    postgres_conn_id=POSTGRES_CONN_ID,
    sql=create_tables_sql,
    dag=dag
)

extract_data = PythonOperator(
    task_id='extract_market_data',
    python_callable=extract_market_data,
    dag=dag
)

load_raw_data = PythonOperator(
    task_id='load_raw_to_postgres',
    python_callable=load_rawdata_to_postgres,
    dag=dag
)

prepare_spark_job = PythonOperator(
    task_id='prepare_spark_transformation',
    python_callable=submit_spark_transformation,
    dag=dag
)

def data_quality_checks(**context):
    """
    Perform basic data quality checks on processed data
    """
    logging.info("Starting data quality checks")
    
    try:
        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        count_query = """
        SELECT COUNT(*) as record_count
        FROM curated_market_data 
        WHERE extraction_timestamp = %s
        """
        
        extraction_timestamp = context['task_instance'].xcom_pull(
            task_ids='prepare_spark_transformation',
            key='spark_params'
        )['extraction_timestamp']
        
        result = postgres_hook.get_first(count_query, parameters=(extraction_timestamp,))
        record_count = result[0] if result else 0
        
        if record_count == 0:
            raise ValueError("No records found in curated data")
        
        if record_count < 45:  
            logging.warning(f"Lower than expected record count: {record_count}")
        
        # Check 2: Data completeness for top coins
        completeness_query = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(current_price) as price_records,
            COUNT(market_cap) as market_cap_records
        FROM curated_market_data 
        WHERE extraction_timestamp = %s
        """
        
        result = postgres_hook.get_first(completeness_query, parameters=(extraction_timestamp,))
        if result:
            total, price_records, market_cap_records = result
            price_completeness = (price_records / total) * 100 if total > 0 else 0
            market_cap_completeness = (market_cap_records / total) * 100 if total > 0 else 0
            
            logging.info(f"Data completeness - Price: {price_completeness:.1f}%, Market Cap: {market_cap_completeness:.1f}%")
            
            if price_completeness < 95:
                raise ValueError(f"Price data completeness too low: {price_completeness:.1f}%")
        
        logging.info("Data quality checks passed successfully")
        
    except Exception as e:
        logging.error(f"Data quality checks failed: {e}")
        raise

quality_checks = PythonOperator(
    task_id='data_quality_checks',
    python_callable=data_quality_checks,
    dag=dag
)

extract_data >> load_raw_data >> prepare_spark_job >> quality_checks