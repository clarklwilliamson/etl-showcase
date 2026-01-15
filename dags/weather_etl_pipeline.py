"""
Weather ETL Pipeline

This DAG demonstrates a complete ETL pipeline that:
1. Extracts weather data from Open-Meteo API (free, no API key required)
2. Transforms data using Apache Spark (aggregations, data quality)
3. Loads results into PostgreSQL data warehouse

Schedule: Daily at 6 AM UTC
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.empty import EmptyOperator
import requests
import json
import os

# Configuration
CITIES = [
    {"name": "New York", "lat": 40.7128, "lon": -74.0060},
    {"name": "Los Angeles", "lat": 34.0522, "lon": -118.2437},
    {"name": "Chicago", "lat": 41.8781, "lon": -87.6298},
    {"name": "Houston", "lat": 29.7604, "lon": -95.3698},
    {"name": "Phoenix", "lat": 33.4484, "lon": -112.0740},
]

DATA_DIR = "/opt/airflow/data"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}


def extract_weather_data(**context):
    """
    Extract weather data from Open-Meteo API.

    This function demonstrates:
    - API integration with error handling
    - Idempotent extraction (same input = same output)
    - Data partitioning by execution date
    """
    execution_date = context["ds"]
    raw_dir = f"{DATA_DIR}/raw/weather/{execution_date}"
    os.makedirs(raw_dir, exist_ok=True)

    all_data = []

    for city in CITIES:
        try:
            # Open-Meteo API - free, no API key required
            url = (
                f"https://api.open-meteo.com/v1/forecast"
                f"?latitude={city['lat']}&longitude={city['lon']}"
                f"&daily=temperature_2m_max,temperature_2m_min,precipitation_sum,"
                f"windspeed_10m_max,weathercode"
                f"&timezone=America/New_York"
                f"&past_days=7"
            )

            response = requests.get(url, timeout=30)
            response.raise_for_status()

            data = response.json()
            data["city"] = city["name"]
            data["extracted_at"] = datetime.utcnow().isoformat()
            all_data.append(data)

            print(f"Successfully extracted data for {city['name']}")

        except requests.RequestException as e:
            print(f"Error extracting data for {city['name']}: {e}")
            raise

    # Write raw data to JSON (for Spark to process)
    output_file = f"{raw_dir}/weather_raw.json"
    with open(output_file, "w") as f:
        json.dump(all_data, f, indent=2)

    print(f"Wrote {len(all_data)} city records to {output_file}")
    return output_file


def validate_extracted_data(**context):
    """
    Validate extracted data before transformation.

    This demonstrates data quality checks at the extraction stage.
    """
    execution_date = context["ds"]
    raw_file = f"{DATA_DIR}/raw/weather/{execution_date}/weather_raw.json"

    with open(raw_file, "r") as f:
        data = json.load(f)

    # Validation checks
    assert len(data) == len(CITIES), f"Expected {len(CITIES)} cities, got {len(data)}"

    for record in data:
        assert "daily" in record, f"Missing 'daily' key in record for {record.get('city')}"
        assert "temperature_2m_max" in record["daily"], "Missing temperature data"
        assert len(record["daily"]["time"]) > 0, "No time series data"

    print(f"Validation passed for {len(data)} city records")
    return True


with DAG(
    dag_id="weather_etl_pipeline",
    default_args=default_args,
    description="ETL pipeline for weather data: Extract from API, Transform with Spark, Load to Postgres",
    schedule_interval="0 6 * * *",  # Daily at 6 AM UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "weather", "spark", "showcase"],
    doc_md=__doc__,
) as dag:

    # Task: Start marker
    start = EmptyOperator(task_id="start")

    # Task: Extract weather data from API
    extract = PythonOperator(
        task_id="extract_weather_data",
        python_callable=extract_weather_data,
        provide_context=True,
    )

    # Task: Validate extracted data
    validate = PythonOperator(
        task_id="validate_extracted_data",
        python_callable=validate_extracted_data,
        provide_context=True,
    )

    # Task: Transform data with Spark
    transform = SparkSubmitOperator(
        task_id="transform_weather_data",
        application="/opt/airflow/spark_jobs/transform_weather.py",
        conn_id="spark_default",
        conf={
            "spark.master": "spark://spark-master:7077",
            "spark.driver.memory": "1g",
            "spark.executor.memory": "1g",
        },
        application_args=["{{ ds }}"],
        name="weather_transform",
    )

    # Task: Create/update target tables
    create_tables = PostgresOperator(
        task_id="create_tables",
        postgres_conn_id="postgres_warehouse",
        sql="sql/create_weather_tables.sql",
    )

    # Task: Load transformed data to warehouse
    load = PostgresOperator(
        task_id="load_to_warehouse",
        postgres_conn_id="postgres_warehouse",
        sql="""
            -- Load daily weather facts
            INSERT INTO fact_daily_weather (
                city_name, date, temp_max, temp_min, precipitation,
                wind_speed_max, weather_code, loaded_at
            )
            SELECT
                city_name, date, temp_max, temp_min, precipitation,
                wind_speed_max, weather_code, NOW()
            FROM staging_weather
            WHERE date = '{{ ds }}'
            ON CONFLICT (city_name, date)
            DO UPDATE SET
                temp_max = EXCLUDED.temp_max,
                temp_min = EXCLUDED.temp_min,
                precipitation = EXCLUDED.precipitation,
                wind_speed_max = EXCLUDED.wind_speed_max,
                weather_code = EXCLUDED.weather_code,
                loaded_at = NOW();
        """,
    )

    # Task: Data quality check on loaded data
    quality_check = PostgresOperator(
        task_id="data_quality_check",
        postgres_conn_id="postgres_warehouse",
        sql="""
            -- Verify data was loaded correctly
            DO $$
            DECLARE
                row_count INTEGER;
            BEGIN
                SELECT COUNT(*) INTO row_count
                FROM fact_daily_weather
                WHERE date = '{{ ds }}';

                IF row_count = 0 THEN
                    RAISE EXCEPTION 'No data loaded for date {{ ds }}';
                END IF;

                RAISE NOTICE 'Quality check passed: % rows for {{ ds }}', row_count;
            END $$;
        """,
    )

    # Task: End marker
    end = EmptyOperator(task_id="end")

    # Define task dependencies
    start >> extract >> validate >> transform >> create_tables >> load >> quality_check >> end
