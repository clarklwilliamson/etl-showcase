-- Weather Data Warehouse Schema
-- Demonstrates dimensional modeling for analytics

-- ===================
-- STAGING TABLES
-- ===================
-- Staging tables are overwritten each run by Spark

CREATE TABLE IF NOT EXISTS staging_weather (
    city_name VARCHAR(100),
    latitude FLOAT,
    longitude FLOAT,
    timezone VARCHAR(50),
    extracted_at TIMESTAMP,
    date DATE,
    temp_max FLOAT,
    temp_min FLOAT,
    precipitation FLOAT,
    wind_speed_max FLOAT,
    weather_code FLOAT,
    temp_range FLOAT,
    weather_category VARCHAR(20),
    processed_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging_weather_summary (
    city_name VARCHAR(100),
    avg_temp_max FLOAT,
    avg_temp_min FLOAT,
    avg_precipitation FLOAT,
    max_wind_speed FLOAT,
    avg_temp_range FLOAT,
    computed_at TIMESTAMP
);


-- ===================
-- DIMENSION TABLES
-- ===================

-- Dimension: Cities
CREATE TABLE IF NOT EXISTS dim_city (
    city_id SERIAL PRIMARY KEY,
    city_name VARCHAR(100) UNIQUE NOT NULL,
    latitude FLOAT,
    longitude FLOAT,
    timezone VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Dimension: Weather Codes (lookup table)
CREATE TABLE IF NOT EXISTS dim_weather_code (
    weather_code INTEGER PRIMARY KEY,
    description VARCHAR(100),
    category VARCHAR(20)
);

-- Populate weather code dimension (WMO codes)
INSERT INTO dim_weather_code (weather_code, description, category) VALUES
    (0, 'Clear sky', 'Clear'),
    (1, 'Mainly clear', 'Clear'),
    (2, 'Partly cloudy', 'Clear'),
    (3, 'Overcast', 'Cloudy'),
    (45, 'Fog', 'Cloudy'),
    (48, 'Depositing rime fog', 'Cloudy'),
    (51, 'Light drizzle', 'Rain'),
    (53, 'Moderate drizzle', 'Rain'),
    (55, 'Dense drizzle', 'Rain'),
    (61, 'Slight rain', 'Rain'),
    (63, 'Moderate rain', 'Rain'),
    (65, 'Heavy rain', 'Rain'),
    (71, 'Slight snow', 'Snow'),
    (73, 'Moderate snow', 'Snow'),
    (75, 'Heavy snow', 'Snow'),
    (80, 'Slight rain showers', 'Rain'),
    (81, 'Moderate rain showers', 'Rain'),
    (82, 'Violent rain showers', 'Severe'),
    (95, 'Thunderstorm', 'Severe'),
    (96, 'Thunderstorm with hail', 'Severe'),
    (99, 'Thunderstorm with heavy hail', 'Severe')
ON CONFLICT (weather_code) DO NOTHING;


-- ===================
-- FACT TABLES
-- ===================

-- Fact: Daily Weather Observations
CREATE TABLE IF NOT EXISTS fact_daily_weather (
    id SERIAL PRIMARY KEY,
    city_name VARCHAR(100) NOT NULL,
    date DATE NOT NULL,
    temp_max FLOAT,
    temp_min FLOAT,
    temp_range FLOAT GENERATED ALWAYS AS (temp_max - temp_min) STORED,
    precipitation FLOAT DEFAULT 0,
    wind_speed_max FLOAT,
    weather_code INTEGER,
    weather_category VARCHAR(20),
    loaded_at TIMESTAMP DEFAULT NOW(),

    -- Composite unique constraint for idempotent loads
    CONSTRAINT unique_city_date UNIQUE (city_name, date)
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_weather_date ON fact_daily_weather(date);
CREATE INDEX IF NOT EXISTS idx_weather_city ON fact_daily_weather(city_name);
CREATE INDEX IF NOT EXISTS idx_weather_category ON fact_daily_weather(weather_category);


-- ===================
-- AGGREGATE TABLES
-- ===================

-- Monthly aggregates (materialized for performance)
CREATE TABLE IF NOT EXISTS agg_monthly_weather (
    id SERIAL PRIMARY KEY,
    city_name VARCHAR(100) NOT NULL,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    avg_temp_max FLOAT,
    avg_temp_min FLOAT,
    total_precipitation FLOAT,
    rainy_days INTEGER,
    max_wind_speed FLOAT,
    computed_at TIMESTAMP DEFAULT NOW(),

    CONSTRAINT unique_city_month UNIQUE (city_name, year, month)
);


-- ===================
-- VIEWS
-- ===================

-- View: Latest weather by city
CREATE OR REPLACE VIEW v_latest_weather AS
SELECT DISTINCT ON (city_name)
    city_name,
    date,
    temp_max,
    temp_min,
    precipitation,
    weather_category
FROM fact_daily_weather
ORDER BY city_name, date DESC;

-- View: 7-day weather trends
CREATE OR REPLACE VIEW v_weekly_trends AS
SELECT
    city_name,
    ROUND(AVG(temp_max)::numeric, 1) as avg_high,
    ROUND(AVG(temp_min)::numeric, 1) as avg_low,
    ROUND(SUM(precipitation)::numeric, 2) as total_precip,
    COUNT(*) FILTER (WHERE weather_category = 'Rain') as rainy_days
FROM fact_daily_weather
WHERE date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY city_name;
