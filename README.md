# Crypto Market ETL Pipeline

An automated ETL pipeline built with Apache Airflow and Spark for extracting, transforming, and loading cryptocurrency market data.

## Architecture

- **Orchestration**: Apache Airflow (Scheduler, Webserver, Worker)
- **Processing**: Apache Spark (PySpark transformations)
- **Storage**: PostgreSQL (metadata + transformed data)
- **Containerization**: Docker + Docker Compose

## Project Structure

```
.
├── Dockerfile                          # Airflow container definition
├── docker-compose.yaml                 # Multi-container orchestration
├── airflow_jars/
│   └── postgresql-42.6.0.jar          # JDBC driver for Postgres
└── dags/
    ├── crypto_market_etl.py           # Main ETL workflow DAG
    └── spark_jobs/
        └── transform_market_data.py   # PySpark transformation script
```

## Quick Start

1. **Start the pipeline**
   ```bash
   docker-compose up -d
   ```

2. **Access Airflow UI**
   ```
   http://localhost:8080
   ```

3. **Trigger the DAG**
   - Navigate to `crypto_market_etl` DAG
   - Click "Trigger DAG"

## ETL Workflow

1. **Extract**: Fetch crypto market data from external APIs (CoinGecko/Binance)
2. **Transform**: Run PySpark job to clean, aggregate, and process raw data
3. **Load**: Store transformed data in PostgreSQL via JDBC

## Tech Stack

- Python 3.x
- Apache Airflow 2.x
- Apache Spark 3.x
- PostgreSQL 14+
- Docker & Docker Compose

## Architecture
<img width="1823" height="1204" alt="image" src="https://github.com/user-attachments/assets/55d977b8-66f4-46f9-8e57-65b12011107f" />
