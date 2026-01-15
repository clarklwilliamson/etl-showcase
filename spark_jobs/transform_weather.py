"""
Spark Transformation Job: Weather Data

This job demonstrates:
- Reading JSON data extracted from APIs
- Data flattening and normalization
- Aggregations and derived metrics
- Data quality transformations
- Writing to staging tables for warehouse loading
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import arrays_zip, avg, col, current_timestamp, explode, when
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import round as spark_round
from pyspark.sql.types import ArrayType, FloatType, StringType, StructField, StructType


def create_spark_session():
    """Create Spark session with PostgreSQL connectivity."""
    return (
        SparkSession.builder.appName("WeatherDataTransform")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
        .getOrCreate()
    )


def read_raw_data(spark, execution_date, data_dir="/opt/data"):
    """Read raw JSON data extracted from weather API."""
    input_path = f"{data_dir}/raw/weather/{execution_date}/weather_raw.json"

    # Define schema for type safety
    schema = StructType(
        [
            StructField("city", StringType(), True),
            StructField("latitude", FloatType(), True),
            StructField("longitude", FloatType(), True),
            StructField("timezone", StringType(), True),
            StructField("extracted_at", StringType(), True),
            StructField(
                "daily",
                StructType(
                    [
                        StructField("time", ArrayType(StringType()), True),
                        StructField("temperature_2m_max", ArrayType(FloatType()), True),
                        StructField("temperature_2m_min", ArrayType(FloatType()), True),
                        StructField("precipitation_sum", ArrayType(FloatType()), True),
                        StructField("windspeed_10m_max", ArrayType(FloatType()), True),
                        StructField("weathercode", ArrayType(FloatType()), True),
                    ]
                ),
                True,
            ),
        ]
    )

    df = spark.read.schema(schema).json(input_path)
    print(f"Read {df.count()} city records from {input_path}")
    return df


def flatten_daily_data(df):
    """
    Flatten nested daily arrays into individual rows.

    Transforms:
        {city: "NYC", daily: {time: [d1,d2], temp_max: [t1,t2]}}
    Into:
        {city: "NYC", date: "d1", temp_max: t1}
        {city: "NYC", date: "d2", temp_max: t2}
    """
    # Zip all daily arrays together
    df_with_zipped = df.withColumn(
        "daily_zipped",
        arrays_zip(
            col("daily.time"),
            col("daily.temperature_2m_max"),
            col("daily.temperature_2m_min"),
            col("daily.precipitation_sum"),
            col("daily.windspeed_10m_max"),
            col("daily.weathercode"),
        ),
    )

    # Explode to get one row per day per city
    df_exploded = df_with_zipped.select(
        col("city").alias("city_name"),
        col("latitude"),
        col("longitude"),
        col("timezone"),
        col("extracted_at"),
        explode(col("daily_zipped")).alias("daily_record"),
    )

    # Extract individual fields
    df_flattened = df_exploded.select(
        col("city_name"),
        col("latitude"),
        col("longitude"),
        col("timezone"),
        col("extracted_at"),
        col("daily_record.time").alias("date"),
        col("daily_record.temperature_2m_max").alias("temp_max"),
        col("daily_record.temperature_2m_min").alias("temp_min"),
        col("daily_record.precipitation_sum").alias("precipitation"),
        col("daily_record.windspeed_10m_max").alias("wind_speed_max"),
        col("daily_record.weathercode").alias("weather_code"),
    )

    print(f"Flattened to {df_flattened.count()} daily records")
    return df_flattened


def add_derived_metrics(df):
    """
    Add calculated fields and data quality transformations.

    Demonstrates:
    - Derived metrics (temperature range)
    - Null handling
    - Data categorization
    """
    df_enriched = (
        df.withColumn("temp_range", spark_round(col("temp_max") - col("temp_min"), 1))
        .withColumn(
            "precipitation",
            when(col("precipitation").isNull(), 0.0).otherwise(col("precipitation")),
        )
        .withColumn(
            "weather_category",
            when(col("weather_code") < 3, "Clear")
            .when(col("weather_code") < 50, "Cloudy")
            .when(col("weather_code") < 70, "Rain")
            .when(col("weather_code") < 80, "Snow")
            .otherwise("Severe"),
        )
        .withColumn("processed_at", current_timestamp())
    )

    return df_enriched


def compute_aggregates(df):
    """
    Compute city-level aggregates for summary table.

    Demonstrates Spark aggregation patterns.
    """
    df_agg = (
        df.groupBy("city_name")
        .agg(
            spark_round(avg("temp_max"), 1).alias("avg_temp_max"),
            spark_round(avg("temp_min"), 1).alias("avg_temp_min"),
            spark_round(avg("precipitation"), 2).alias("avg_precipitation"),
            spark_max("wind_speed_max").alias("max_wind_speed"),
            spark_round(avg("temp_range"), 1).alias("avg_temp_range"),
        )
        .withColumn("computed_at", current_timestamp())
    )

    return df_agg


def write_to_postgres(df, table_name, jdbc_url, properties):
    """Write DataFrame to PostgreSQL staging table."""
    df.write.jdbc(
        url=jdbc_url,
        table=table_name,
        mode="overwrite",  # Staging tables get replaced
        properties=properties,
    )
    print(f"Wrote {df.count()} rows to {table_name}")


def main(execution_date):
    """Main transformation pipeline."""
    print(f"Starting weather data transformation for {execution_date}")

    # Initialize Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # PostgreSQL connection config
    jdbc_url = "jdbc:postgresql://postgres-warehouse:5432/warehouse"
    jdbc_properties = {
        "user": "warehouse",
        "password": "warehouse",
        "driver": "org.postgresql.Driver",
    }

    try:
        # ETL Pipeline
        # 1. Extract: Read raw data
        df_raw = read_raw_data(spark, execution_date)

        # 2. Transform: Flatten nested structure
        df_flat = flatten_daily_data(df_raw)

        # 3. Transform: Add derived metrics
        df_enriched = add_derived_metrics(df_flat)

        # 4. Transform: Compute aggregates
        df_agg = compute_aggregates(df_enriched)

        # 5. Load: Write to staging tables
        write_to_postgres(df_enriched, "staging_weather", jdbc_url, jdbc_properties)
        write_to_postgres(df_agg, "staging_weather_summary", jdbc_url, jdbc_properties)

        # Show sample output
        print("\n=== Sample Transformed Data ===")
        df_enriched.show(5, truncate=False)

        print("\n=== City Aggregates ===")
        df_agg.show(truncate=False)

        print(f"Transformation complete for {execution_date}")

    finally:
        spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: transform_weather.py <execution_date>")
        sys.exit(1)

    execution_date = sys.argv[1]
    main(execution_date)
