"""
Unit tests for Spark transformation jobs.

These tests validate the transformation logic without requiring
the full Airflow/Docker infrastructure.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    ArrayType,
)
import json
import tempfile
import os


@pytest.fixture(scope="module")
def spark():
    """Create a SparkSession for testing."""
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("test_weather_transform")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def sample_weather_data():
    """Sample weather data matching API response structure."""
    return [
        {
            "city": "New York",
            "latitude": 40.7128,
            "longitude": -74.0060,
            "timezone": "America/New_York",
            "extracted_at": "2024-01-15T10:00:00",
            "daily": {
                "time": ["2024-01-14", "2024-01-15"],
                "temperature_2m_max": [45.2, 42.8],
                "temperature_2m_min": [32.1, 30.5],
                "precipitation_sum": [0.0, 0.25],
                "windspeed_10m_max": [15.3, 22.1],
                "weathercode": [1.0, 61.0],
            },
        },
        {
            "city": "Los Angeles",
            "latitude": 34.0522,
            "longitude": -118.2437,
            "timezone": "America/Los_Angeles",
            "extracted_at": "2024-01-15T10:00:00",
            "daily": {
                "time": ["2024-01-14", "2024-01-15"],
                "temperature_2m_max": [72.5, 75.0],
                "temperature_2m_min": [55.2, 58.1],
                "precipitation_sum": [0.0, 0.0],
                "windspeed_10m_max": [8.2, 6.5],
                "weathercode": [0.0, 2.0],
            },
        },
    ]


@pytest.fixture
def raw_data_path(sample_weather_data):
    """Create temporary file with sample data."""
    with tempfile.TemporaryDirectory() as tmpdir:
        data_path = os.path.join(tmpdir, "weather_raw.json")
        with open(data_path, "w") as f:
            json.dump(sample_weather_data, f)
        yield data_path


class TestWeatherDataSchema:
    """Tests for data schema validation."""

    def test_raw_data_has_required_fields(self, sample_weather_data):
        """Verify sample data has all required fields."""
        required_fields = ["city", "latitude", "longitude", "daily"]
        daily_required = [
            "time",
            "temperature_2m_max",
            "temperature_2m_min",
            "precipitation_sum",
        ]

        for record in sample_weather_data:
            for field in required_fields:
                assert field in record, f"Missing field: {field}"
            for field in daily_required:
                assert field in record["daily"], f"Missing daily field: {field}"

    def test_data_types_are_correct(self, sample_weather_data):
        """Verify data types match expected schema."""
        record = sample_weather_data[0]
        assert isinstance(record["city"], str)
        assert isinstance(record["latitude"], float)
        assert isinstance(record["daily"]["time"], list)
        assert isinstance(record["daily"]["temperature_2m_max"][0], float)


class TestSparkTransformations:
    """Tests for Spark transformation logic."""

    def test_read_json_data(self, spark, raw_data_path):
        """Test reading raw JSON data into DataFrame."""
        df = spark.read.json(raw_data_path)
        assert df.count() == 2
        assert "city" in df.columns
        assert "daily" in df.columns

    def test_flatten_daily_arrays(self, spark, raw_data_path):
        """Test flattening nested daily arrays."""
        from pyspark.sql.functions import col, explode, arrays_zip

        df = spark.read.json(raw_data_path)

        # Zip daily arrays
        df_zipped = df.withColumn(
            "daily_zipped",
            arrays_zip(
                col("daily.time"),
                col("daily.temperature_2m_max"),
                col("daily.temperature_2m_min"),
            ),
        )

        # Explode to rows
        df_flat = df_zipped.select(
            col("city"),
            explode(col("daily_zipped")).alias("daily_record"),
        )

        # 2 cities x 2 days = 4 rows
        assert df_flat.count() == 4

    def test_derived_metrics_calculation(self, spark):
        """Test temperature range calculation."""
        from pyspark.sql.functions import col, round as spark_round

        data = [
            ("NYC", 45.2, 32.1),
            ("LA", 72.5, 55.2),
        ]
        df = spark.createDataFrame(data, ["city", "temp_max", "temp_min"])

        df_with_range = df.withColumn(
            "temp_range", spark_round(col("temp_max") - col("temp_min"), 1)
        )

        results = {row.city: row.temp_range for row in df_with_range.collect()}
        assert results["NYC"] == 13.1
        assert results["LA"] == 17.3

    def test_weather_category_mapping(self, spark):
        """Test weather code to category mapping."""
        from pyspark.sql.functions import col, when

        data = [(0.0,), (3.0,), (61.0,), (73.0,), (95.0,)]
        df = spark.createDataFrame(data, ["weather_code"])

        df_categorized = df.withColumn(
            "category",
            when(col("weather_code") < 3, "Clear")
            .when(col("weather_code") < 50, "Cloudy")
            .when(col("weather_code") < 70, "Rain")
            .when(col("weather_code") < 80, "Snow")
            .otherwise("Severe"),
        )

        categories = [row.category for row in df_categorized.collect()]
        assert categories == ["Clear", "Cloudy", "Rain", "Snow", "Severe"]

    def test_null_precipitation_handling(self, spark):
        """Test null values are handled correctly."""
        from pyspark.sql.functions import col, when

        data = [(1.5,), (None,), (0.0,)]
        df = spark.createDataFrame(data, ["precipitation"])

        df_handled = df.withColumn(
            "precipitation",
            when(col("precipitation").isNull(), 0.0).otherwise(col("precipitation")),
        )

        values = [row.precipitation for row in df_handled.collect()]
        assert values == [1.5, 0.0, 0.0]


class TestAggregations:
    """Tests for aggregation logic."""

    def test_city_aggregates(self, spark):
        """Test city-level aggregations."""
        from pyspark.sql.functions import avg, max as spark_max, round as spark_round

        data = [
            ("NYC", 45.0, 32.0, 0.1),
            ("NYC", 42.0, 30.0, 0.2),
            ("LA", 72.0, 55.0, 0.0),
            ("LA", 75.0, 58.0, 0.0),
        ]
        df = spark.createDataFrame(
            data, ["city", "temp_max", "temp_min", "precipitation"]
        )

        df_agg = df.groupBy("city").agg(
            spark_round(avg("temp_max"), 1).alias("avg_temp_max"),
            spark_round(avg("precipitation"), 2).alias("avg_precip"),
        )

        results = {row.city: row for row in df_agg.collect()}
        assert results["NYC"].avg_temp_max == 43.5
        assert results["LA"].avg_temp_max == 73.5
        assert results["NYC"].avg_precip == 0.15
        assert results["LA"].avg_precip == 0.0
