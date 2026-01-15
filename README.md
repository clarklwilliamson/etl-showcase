# ETL Pipeline Showcase

A production-ready ETL pipeline demonstrating modern data engineering practices with **Apache Airflow**, **Apache Spark**, and **PostgreSQL**.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           ETL Pipeline Architecture                          │
└─────────────────────────────────────────────────────────────────────────────┘

    ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
    │  External    │     │   Apache     │     │   Apache     │
    │  APIs        │────▶│   Airflow    │────▶│   Spark      │
    │  (Weather)   │     │  (Orchestr.) │     │  (Transform) │
    └──────────────┘     └──────────────┘     └──────────────┘
                                                     │
                                                     ▼
                              ┌──────────────────────────────────┐
                              │         PostgreSQL               │
                              │  ┌────────────────────────────┐  │
                              │  │  Staging    │    Warehouse │  │
                              │  │  Tables     │    (Facts +  │  │
                              │  │             │    Dims)     │  │
                              │  └────────────────────────────┘  │
                              └──────────────────────────────────┘

    Data Flow:
    ─────────
    1. EXTRACT:  Airflow pulls weather data from Open-Meteo API
    2. VALIDATE: Data quality checks before transformation
    3. TRANSFORM: Spark flattens, enriches, and aggregates
    4. LOAD:     Upsert to dimensional warehouse tables
    5. VERIFY:   Post-load quality checks
```

## Features

- **Orchestration**: Apache Airflow with DAGs following best practices
  - Idempotent tasks (re-runnable without side effects)
  - Proper error handling and retries
  - Task dependencies and SLAs

- **Transformation**: Apache Spark for scalable data processing
  - JSON parsing and flattening
  - Derived metrics and data enrichment
  - Aggregations for analytics

- **Storage**: PostgreSQL data warehouse
  - Dimensional modeling (facts + dimensions)
  - Staging tables for incremental loads
  - Materialized views for performance

- **Data Quality**: Built-in validation
  - Pre-transformation validation
  - Post-load verification
  - Null handling and type safety

## Quick Start

### Prerequisites
- Docker & Docker Compose
- 4GB+ RAM available for containers

### Launch the Stack

```bash
# Clone the repository
git clone https://github.com/YOUR_USERNAME/etl-showcase.git
cd etl-showcase

# Start all services
docker-compose up -d

# Wait for services to initialize (~2 minutes)
docker-compose logs -f airflow-init
```

### Access the UIs

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | admin / admin |
| Spark Master | http://localhost:8081 | - |
| PostgreSQL | localhost:5433 | warehouse / warehouse |

### Run the Pipeline

1. Open Airflow UI at http://localhost:8080
2. Enable the `weather_etl_pipeline` DAG
3. Trigger a manual run or wait for scheduled execution
4. Monitor task progress in the Graph view

## Project Structure

```
etl-showcase/
├── dags/                          # Airflow DAG definitions
│   └── weather_etl_pipeline.py    # Main ETL pipeline
├── spark_jobs/                    # Spark transformation jobs
│   └── transform_weather.py       # Weather data transformer
├── sql/                           # Database schemas
│   ├── create_weather_tables.sql  # Table definitions
│   └── init.sql                   # Initialization script
├── data/                          # Data storage (gitignored)
│   ├── raw/                       # Extracted raw data
│   └── processed/                 # Transformed data
├── config/                        # Configuration files
├── tests/                         # Unit and integration tests
├── docker-compose.yml             # Service definitions
├── Dockerfile.airflow             # Custom Airflow image
└── requirements.txt               # Python dependencies
```

## Pipeline Details

### Weather ETL Pipeline

**Schedule**: Daily at 6 AM UTC

**Tasks**:
1. `extract_weather_data` - Pull from Open-Meteo API for 5 US cities
2. `validate_extracted_data` - Check data completeness
3. `transform_weather_data` - Spark job for flattening and enrichment
4. `create_tables` - Ensure warehouse schema exists
5. `load_to_warehouse` - Upsert to fact tables
6. `data_quality_check` - Verify loaded data

**Data Sources**:
- [Open-Meteo API](https://open-meteo.com/) - Free weather data, no API key required

### Database Schema

```
                    ┌─────────────────┐
                    │   dim_city      │
                    ├─────────────────┤
                    │ city_id (PK)    │
                    │ city_name       │
                    │ latitude        │
                    │ longitude       │
                    └────────┬────────┘
                             │
    ┌────────────────────────┼────────────────────────┐
    │                        │                        │
    ▼                        ▼                        ▼
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│ staging_weather │  │fact_daily_weather│ │agg_monthly_weather│
├─────────────────┤  ├─────────────────┤  ├─────────────────┤
│ city_name       │  │ id (PK)         │  │ id (PK)         │
│ date            │  │ city_name (FK)  │  │ city_name (FK)  │
│ temp_max        │  │ date            │  │ year, month     │
│ temp_min        │  │ temp_max/min    │  │ avg_temp_*      │
│ precipitation   │  │ precipitation   │  │ total_precip    │
│ weather_code    │  │ weather_code    │  │ rainy_days      │
└─────────────────┘  └─────────────────┘  └─────────────────┘
     (Staging)            (Fact)             (Aggregate)
```

## Extending the Pipeline

### Add a New Data Source

1. Create extraction function in `dags/`
2. Add Spark transformation in `spark_jobs/`
3. Define schema in `sql/`
4. Wire up DAG tasks

### Add Data Quality with Great Expectations

```python
# Example: Add to DAG after loading
from great_expectations_provider.operators.great_expectations import (
    GreatExpectationsOperator
)

quality_check = GreatExpectationsOperator(
    task_id="great_expectations_check",
    data_context_root_dir="/opt/airflow/great_expectations",
    checkpoint_name="weather_checkpoint",
)
```

## Development

### Run Spark Jobs Locally

```bash
# Enter Spark container
docker exec -it spark-master bash

# Run transformation manually
spark-submit /opt/spark-jobs/transform_weather.py 2024-01-15
```

### Query the Warehouse

```bash
# Connect to PostgreSQL
docker exec -it postgres-warehouse psql -U warehouse -d warehouse

# Example queries
SELECT * FROM v_latest_weather;
SELECT * FROM v_weekly_trends;
```

### Stop the Stack

```bash
docker-compose down

# Remove volumes (full reset)
docker-compose down -v
```

## Technologies

- **Apache Airflow 2.8** - Workflow orchestration
- **Apache Spark 3.5** - Distributed data processing
- **PostgreSQL 15** - Data warehouse
- **Python 3.11** - Pipeline logic
- **Docker Compose** - Container orchestration

## License

MIT
