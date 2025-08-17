# dags/spark_jobs/transform_market_data.py
"""
PySpark job for transforming CoinGecko market data
Converts raw JSON to curated Parquet with schema validation and partitioning
"""

import argparse
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.functions import explode, col

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MarketDataTransformer:
    """
    Handles transformation of raw CoinGecko market data
    """
    
    def __init__(self, spark_session):
        self.spark = spark_session
        self.postgres_url = "jdbc:postgresql://postgres:5432/airflow"
        self.postgres_properties = {
            "user": "airflow",
            "password": "airflow",
            "driver": "org.postgresql.Driver"
        }
    
    def create_market_data_schema(self):
        """
        Define schema for market data validation
        """
        return StructType([
            StructField("id", StringType(), False),
            StructField("symbol", StringType(), False),
            StructField("name", StringType(), False),
            StructField("image", StringType(), True),
            StructField("current_price", DoubleType(), True),
            StructField("market_cap", LongType(), True),
            StructField("market_cap_rank", IntegerType(), True),
            StructField("fully_diluted_valuation", LongType(), True),
            StructField("total_volume", DoubleType(), True),
            StructField("high_24h", DoubleType(), True),
            StructField("low_24h", DoubleType(), True),
            StructField("price_change_24h", DoubleType(), True),
            StructField("price_change_percentage_24h", DoubleType(), True),
            StructField("market_cap_change_24h", LongType(), True),
            StructField("market_cap_change_percentage_24h", DoubleType(), True),
            StructField("circulating_supply", DoubleType(), True),
            StructField("total_supply", DoubleType(), True),
            StructField("max_supply", DoubleType(), True),
            StructField("ath", DoubleType(), True),
            StructField("ath_change_percentage", DoubleType(), True),
            StructField("ath_date", TimestampType(), True),
            StructField("atl", DoubleType(), True),
            StructField("atl_change_percentage", DoubleType(), True),
            StructField("atl_date", TimestampType(), True),
            StructField("roi", StructType([
                StructField("times", DoubleType(), True),
                StructField("currency", StringType(), True),
                StructField("percentage", DoubleType(), True)
            ]), True),
            StructField("last_updated", TimestampType(), True)
        ])
    
    def extract_raw_data(self, extraction_timestamp):
        """
        Extract raw JSON data from PostgreSQL
        """
        logger.info(f"Extracting raw data for timestamp: {extraction_timestamp}")
        
        query = f"""
        (SELECT raw_json_data 
         FROM raw_market_data 
         WHERE extraction_timestamp = '{extraction_timestamp}') as raw_data
        """
        
        raw_df = self.spark.read \
            .jdbc(self.postgres_url, query, properties=self.postgres_properties)
        
        if raw_df.count() == 0:
            raise ValueError(f"No raw data found for timestamp: {extraction_timestamp}")
        
        # Parse JSON data
        json_data = raw_df.collect()[0]['raw_json_data']
        
        # Create DataFrame from JSON string
        json_rdd = self.spark.sparkContext.parallelize([json_data])
        df = self.spark.read.json(json_rdd)
        
        # Explode the array to get individual records
        if "data" in df.columns:  
            exploded_df = df.select(explode(col("data")).alias("market_data"))
            market_df = exploded_df.select("market_data.*")
        else:
            # JSON is already a list of objects, no need to explode
            market_df = df
        
        logger.info(f"Successfully extracted {market_df.count()} records")
        return market_df
    
    def transform_market_data(self, raw_df, extraction_timestamp, execution_date):
        """
        Apply transformations to market data
        """
        logger.info("Starting data transformations")
        
        # Add extraction metadata first
        df_with_metadata = raw_df.withColumn("extraction_timestamp", 
                                           lit(extraction_timestamp).cast(TimestampType())) \
                                  .withColumn("partition_date", 
                                            to_date(lit(execution_date))) \
                                  .withColumn("created_at", current_timestamp())
        
        # Data quality filtering - do this early
        df_filtered = df_with_metadata.filter(
            col("id").isNotNull() & 
            col("symbol").isNotNull() & 
            col("name").isNotNull()
        )
        
        # First, select base columns and handle timestamp conversions
        base_df = df_filtered.select(
            col("id"),
            col("symbol"),
            col("name"),
            col("current_price"),
            col("market_cap"),
            col("market_cap_rank"),
            col("fully_diluted_valuation"),
            col("total_volume"),
            col("high_24h"),
            col("low_24h"),
            col("price_change_24h"),
            col("price_change_percentage_24h"),
            col("market_cap_change_24h"),
            col("market_cap_change_percentage_24h"),
            col("circulating_supply"),
            col("total_supply"),
            col("max_supply"),
            col("ath"),
            col("ath_change_percentage"),
            # Handle timestamp conversion more safely
            when(col("ath_date").isNotNull(), to_timestamp("ath_date")).alias("ath_date"),
            col("atl"),
            col("atl_change_percentage"),
            when(col("atl_date").isNotNull(), to_timestamp("atl_date")).alias("atl_date"),
            when(col("last_updated").isNotNull(), to_timestamp("last_updated")).alias("last_updated"),
            col("extraction_timestamp"),
            col("partition_date"),
            col("created_at")
        )
        
        # Now add derived metrics step by step
        transformed_df = base_df.withColumn(
            "price_volatility_24h",
            when((col("low_24h").isNotNull()) & (col("high_24h").isNotNull()) & (col("low_24h") > 0), 
                 ((col("high_24h") - col("low_24h")) / col("low_24h")) * 100
            ).otherwise(None)
        )
        
        # Add market cap in billions
        transformed_df = transformed_df.withColumn(
            "market_cap_billions",
            when(col("market_cap").isNotNull(),
                 round(col("market_cap") / 1e9, 3)
            ).otherwise(None)
        )
        
        # Add volume to market cap ratio
        transformed_df = transformed_df.withColumn(
            "volume_market_cap_ratio",
            when((col("market_cap").isNotNull()) & (col("market_cap") > 0) & (col("total_volume").isNotNull()),
                 round(col("total_volume") / col("market_cap"), 4)
            ).otherwise(None)
        )
        
        # Add market cap category
        transformed_df = transformed_df.withColumn(
            "market_cap_category",
            when(col("market_cap_rank").isNull(), "unranked")
            .when(col("market_cap_rank") <= 10, "large_cap")
            .when(col("market_cap_rank") <= 50, "mid_cap")
            .when(col("market_cap_rank") <= 200, "small_cap")
            .otherwise("micro_cap")
        )
        
        logger.info(f"Transformation completed for {transformed_df.count()} records")
        return transformed_df
    
    def write_to_parquet(self, df, output_path):
        """
        Write transformed data to partitioned Parquet files
        """
        logger.info(f"Writing data to Parquet: {output_path}")
        
        # Write partitioned by date
        df.write \
          .mode("append") \
          .partitionBy("partition_date") \
          .option("compression", "snappy") \
          .parquet(f"{output_path}/market_data_curated")
        
        logger.info("Successfully wrote data to Parquet")
    
    def write_to_postgres(self, df, extraction_timestamp):
        """
        Write transformed data back to PostgreSQL
        """
        logger.info("Writing transformed data to PostgreSQL")
        
        # Debug: Print the schema of DataFrame being written
        logger.info("DataFrame schema being written to PostgreSQL:")
        df.printSchema()
        
        # Select only the columns that exist in the target table
        # Exclude derived columns that might not exist in the target table
        base_columns = [
            "id", "symbol", "name", "current_price", "market_cap", "market_cap_rank",
            "fully_diluted_valuation", "total_volume", "high_24h", "low_24h",
            "price_change_24h", "price_change_percentage_24h", "market_cap_change_24h",
            "market_cap_change_percentage_24h", "circulating_supply", "total_supply",
            "max_supply", "ath", "ath_change_percentage", "ath_date", "atl",
            "atl_change_percentage", "atl_date", "last_updated", "extraction_timestamp",
            "partition_date", "created_at"
        ]
        
        # Only select columns that exist in the DataFrame
        existing_columns = [col for col in base_columns if col in df.columns]
        df_to_write = df.select(*existing_columns)
        
        # Remove existing data for this extraction timestamp to avoid duplicates
        delete_query = f"""
        DELETE FROM curated_market_data 
        WHERE extraction_timestamp = '{extraction_timestamp}'
        """
        
        try:
            # Note: In production, use a proper connection pool
            import psycopg2
            conn = psycopg2.connect(
                host="postgres",
                database="airflow", 
                user="airflow",
                password="airflow"
            )
            with conn.cursor() as cur:
                cur.execute(delete_query)
                conn.commit()
            conn.close()
            
            # Write new data
            df_to_write.write \
              .mode("append") \
              .jdbc(self.postgres_url, "curated_market_data", properties=self.postgres_properties)
            
            logger.info("Successfully wrote data to PostgreSQL")
            
        except Exception as e:
            logger.error(f"Error writing to PostgreSQL: {e}")
            # If the table doesn't exist or has schema issues, create a simple version
            logger.info("Attempting to write only core columns...")
            core_columns = ["id", "symbol", "name", "current_price", "market_cap", 
                          "market_cap_rank", "extraction_timestamp", "partition_date"]
            core_df = df.select(*[col for col in core_columns if col in df.columns])
            
            core_df.write \
              .mode("append") \
              .jdbc(self.postgres_url, "curated_market_data", properties=self.postgres_properties)
            
            logger.info("Successfully wrote core data to PostgreSQL")
    
    def generate_data_summary(self, df):
        """
        Generate summary statistics for monitoring
        """
        logger.info("Generating data summary")
        
        summary_stats = df.agg(
            count("*").alias("total_records"),
            countDistinct("symbol").alias("unique_symbols"),
            avg("current_price").alias("avg_price"),
            max("market_cap").alias("max_market_cap"),
            min("market_cap_rank").alias("top_rank")
        ).collect()[0]
        
        # Price distribution
        price_distribution = df.select(
            when(col("current_price").isNull(), "null_price")
            .when(col("current_price") < 1, "under_1")
            .when(col("current_price") < 100, "1_to_100") 
            .when(col("current_price") < 1000, "100_to_1000")
            .otherwise("over_1000").alias("price_range")
        ).groupBy("price_range").count().collect()
        
        logger.info(f"Summary Stats: {summary_stats.asDict()}")
        logger.info(f"Price Distribution: {[row.asDict() for row in price_distribution]}")
        
        return summary_stats, price_distribution

def main():
    """
    Main function to orchestrate the transformation process
    """
    parser = argparse.ArgumentParser(description='Transform CoinGecko market data')
    parser.add_argument('--extraction_timestamp', required=True, help='Extraction timestamp')
    parser.add_argument('--execution_date', required=True, help='Execution date')
    parser.add_argument('--output_path', required=True, help='Output path for Parquet files')
    
    args = parser.parse_args()
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("CoinGecko Market Data Transformation") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.jars", "/opt/airflow/jars/postgresql-42.6.0.jar") \
        .getOrCreate()

    try:
        # Initialize transformer
        transformer = MarketDataTransformer(spark)
        
        # Extract raw data
        raw_df = transformer.extract_raw_data(args.extraction_timestamp)
        
        # Debug: Print schema of raw data
        logger.info("Raw DataFrame schema:")
        raw_df.printSchema()
        
        # Transform data
        transformed_df = transformer.transform_market_data(
            raw_df, 
            args.extraction_timestamp,
            args.execution_date
        )
        
        # Cache the DataFrame for multiple operations
        transformed_df.cache()
        
        # Debug: Print schema of transformed data and verify columns exist
        logger.info("Transformed DataFrame schema:")
        transformed_df.printSchema()
        logger.info(f"Transformed DataFrame columns: {transformed_df.columns}")
        
        # Verify the DataFrame has data
        record_count = transformed_df.count()
        logger.info(f"Transformed DataFrame has {record_count} records")
        
        if record_count > 0:
            # Show sample data
            logger.info("Sample transformed data:")
            transformed_df.show(5, truncate=False)
        
        # Generate summary for monitoring
        transformer.generate_data_summary(transformed_df)
        
        # Write to Parquet
        transformer.write_to_parquet(transformed_df, args.output_path)
        
        # Write to PostgreSQL
        transformer.write_to_postgres(transformed_df, args.extraction_timestamp)
        
        logger.info("Transformation pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"Transformation pipeline failed: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()