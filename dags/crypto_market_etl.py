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
    start_date=datetime.now(),   # start from now
    catchup=False,
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

spark_transform = BashOperator(
    task_id='spark_transformation',
    bash_command="""
    # Set variables - try /opt location first, fall back to /tmp
    JARS_DIR="/opt/***/jars"
    POSTGRES_JAR="/opt/***/jars/postgresql-42.6.0.jar"
    
    # Create jars directory with proper permissions
    echo "Creating jars directory: $JARS_DIR"
    mkdir -p "$JARS_DIR"
    
    # Check if directory creation was successful
    if [ ! -d "$JARS_DIR" ]; then
        echo "✗ Failed to create jars directory: $JARS_DIR"
        echo "Trying alternative location..."
        JARS_DIR="/tmp/jars"
        POSTGRES_JAR="/tmp/jars/postgresql-42.6.0.jar"
        mkdir -p "$JARS_DIR"
        if [ ! -d "$JARS_DIR" ]; then
            echo "✗ Failed to create alternative directory: $JARS_DIR"
            exit 1
        fi
    fi
    
    echo "✓ Directory ready: $JARS_DIR"
    ls -la "$JARS_DIR" || echo "Directory is empty"
    
    # Check if PostgreSQL JAR exists, if not download it
    if [ ! -f "$POSTGRES_JAR" ]; then
        echo "✗ PostgreSQL JAR not found, downloading to: $POSTGRES_JAR"
        
        # Try wget first
        if command -v wget >/dev/null 2>&1; then
            echo "Using wget to download..."
            wget -v -O "$POSTGRES_JAR" \
                "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar"
            DOWNLOAD_SUCCESS=$?
        elif command -v curl >/dev/null 2>&1; then
            echo "Using curl to download..."
            curl -v -L -o "$POSTGRES_JAR" \
                "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar"
            DOWNLOAD_SUCCESS=$?
        else
            echo "✗ Neither wget nor curl available"
            exit 1
        fi
        
        if [ $DOWNLOAD_SUCCESS -eq 0 ] && [ -f "$POSTGRES_JAR" ] && [ -s "$POSTGRES_JAR" ]; then
            echo "✓ PostgreSQL JAR downloaded successfully"
            ls -la "$POSTGRES_JAR"
        else
            echo "✗ Download failed or file is empty"
            ls -la "$JARS_DIR"
            rm -f "$POSTGRES_JAR"
            exit 1
        fi
    else
        echo "✓ PostgreSQL JAR already exists"
        ls -la "$POSTGRES_JAR"
    fi
    
    # Verify the JAR file is valid
    if [ -f "$POSTGRES_JAR" ] && [ -s "$POSTGRES_JAR" ]; then
        echo "✓ PostgreSQL JAR file is valid (size: $(du -h "$POSTGRES_JAR" | cut -f1))"
    else
        echo "✗ PostgreSQL JAR file is invalid or empty"
        exit 1
    fi
    
    echo "=== Running Spark Job ==="
    # Run Spark with explicit JAR inclusion
    spark-submit \
        --master local[*] \
        --driver-memory 1g \
        --executor-memory 1g \
        --jars "$POSTGRES_JAR" \
        --driver-class-path "$POSTGRES_JAR" \
        --conf spark.sql.adaptive.enabled=true \
        --conf spark.sql.adaptive.coalescePartitions.enabled=true \
        --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
        --conf spark.sql.adaptive.localShuffleReader.enabled=false \
        /opt/***/dags/spark_jobs/transform_market_data.py \
        --extraction_timestamp "{{ (ti.xcom_pull(task_ids='prepare_spark_transformation', key='spark_params') or {}).get('extraction_timestamp', ds) }}" \
        --execution_date "{{ (ti.xcom_pull(task_ids='prepare_spark_transformation', key='spark_params') or {}).get('execution_date', ds) }}" \
        --output_path "{{ (ti.xcom_pull(task_ids='prepare_spark_transformation', key='spark_params') or {}).get('output_path', '/opt/***/data') }}"
    
    echo "Spark job completed with exit code: $?"
    """,
    dag=dag
)
def data_quality_checks(**context):
    """
    Alternative approach: Check for most recent data instead of exact timestamp
    """
    logging.info("Starting alternative data quality checks")
    
    try:
        postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        # Get the most recent extraction timestamp from the table
        latest_query = """
        SELECT 
            extraction_timestamp,
            COUNT(*) as record_count
        FROM curated_market_data 
        WHERE extraction_timestamp >= NOW() - INTERVAL '30 minutes'
        GROUP BY extraction_timestamp 
        ORDER BY extraction_timestamp DESC 
        LIMIT 1
        """
        
        result = postgres_hook.get_first(latest_query)
        
        if not result:
            raise ValueError("No recent data found in curated_market_data (last 30 minutes)")
        
        latest_timestamp, record_count = result
        logging.info(f"Using most recent data: {latest_timestamp} with {record_count} records")
        
        if record_count < 45:
            logging.warning(f"Lower than expected record count: {record_count}")
        
        # Continue with completeness checks using the latest timestamp
        completeness_query = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(current_price) as price_records,
            COUNT(market_cap) as market_cap_records
        FROM curated_market_data 
        WHERE extraction_timestamp = %s
        """
        
        result = postgres_hook.get_first(completeness_query, parameters=(latest_timestamp,))
        if result:
            total, price_records, market_cap_records = result
            price_completeness = (price_records / total) * 100 if total > 0 else 0
            market_cap_completeness = (market_cap_records / total) * 100 if total > 0 else 0
            
            logging.info(f"Data completeness - Price: {price_completeness:.1f}%, Market Cap: {market_cap_completeness:.1f}%")
            
            if price_completeness < 95:
                raise ValueError(f"Price data completeness too low: {price_completeness:.1f}%")
        
        logging.info("Alternative data quality checks passed successfully")
        
    except Exception as e:
        logging.error(f"Alternative data quality checks failed: {e}")
        raise
quality_checks = PythonOperator(
    task_id='data_quality_checks',
    python_callable=data_quality_checks,
    dag=dag
)

extract_data >> load_raw_data >> prepare_spark_job >> spark_transform >> quality_checks