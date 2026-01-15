#!/bin/bash
# Setup Airflow connections after init

set -e

echo "Setting up Airflow connections..."

# Wait for Airflow to be ready
sleep 10

# Create PostgreSQL warehouse connection
docker exec airflow-webserver airflow connections add 'postgres_warehouse' \
    --conn-type 'postgres' \
    --conn-host 'postgres-warehouse' \
    --conn-port 5432 \
    --conn-schema 'warehouse' \
    --conn-login 'warehouse' \
    --conn-password 'warehouse' \
    || echo "Connection postgres_warehouse already exists"

# Create Spark connection
docker exec airflow-webserver airflow connections add 'spark_default' \
    --conn-type 'spark' \
    --conn-host 'spark://spark-master' \
    --conn-port 7077 \
    || echo "Connection spark_default already exists"

echo "Connections setup complete!"
