-- Database initialization script
-- This runs automatically when the PostgreSQL container starts

\echo 'Initializing warehouse database...'

-- Run table creation
\i /docker-entrypoint-initdb.d/create_weather_tables.sql

\echo 'Database initialization complete!'
