-- ============================================================================
-- SILVER LAYER - Cleaned and Deduplicated Data
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS silver;

-- Silver: Inverter Cleaned Data
CREATE TABLE IF NOT EXISTS silver.inverter_cleaned (
    id BIGSERIAL PRIMARY KEY,
    plant_id UUID NOT NULL,
    inverter_id VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    state INTEGER,
    inverter_temp_C NUMERIC(10, 2),
    ac_power_kW NUMERIC(10, 3),
    ac_freq_Hz NUMERIC(10, 2),
    dc_power_kW NUMERIC(10, 3),
    dc_voltage_V NUMERIC(8, 2),
    dc_current_A NUMERIC(8, 2),
    instant_efficiency NUMERIC(5, 4),
    efficiency_flag VARCHAR(20),
    healthy_strings INTEGER,
    failed_strings INTEGER,
    active_failures INTEGER,
    failure_types JSONB,
    source_bronze_id BIGINT REFERENCES bronze.inverter_raw(id),
    processed_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    data_quality_flag VARCHAR(50) DEFAULT 'valid',
    anomaly_flag BOOLEAN DEFAULT FALSE,
    CONSTRAINT uq_inverter_cleaned UNIQUE (plant_id, inverter_id, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_inverter_cleaned_id ON silver.inverter_cleaned(inverter_id);
CREATE INDEX IF NOT EXISTS idx_inverter_cleaned_timestamp ON silver.inverter_cleaned(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_inverter_cleaned_quality ON silver.inverter_cleaned(data_quality_flag);

-- Silver: POI Meter Cleaned Data
CREATE TABLE IF NOT EXISTS silver.poi_meter_cleaned (
    id BIGSERIAL PRIMARY KEY,
    plant_id UUID NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    export_active_power_kW NUMERIC(12, 2),
    import_active_power_kW NUMERIC(12, 2),
    reactive_power_kVAr NUMERIC(12, 2),
    grid_voltage_l1_V NUMERIC(8, 2),
    grid_voltage_l2_V NUMERIC(8, 2),
    grid_voltage_l3_V NUMERIC(8, 2),
    grid_frequency_Hz NUMERIC(6, 3),
    power_factor NUMERIC(5, 3),
    active_failures INTEGER,
    connection_issues BOOLEAN,
    source_bronze_id BIGINT REFERENCES bronze.poi_meter_raw(id),
    processed_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    data_quality_flag VARCHAR(50) DEFAULT 'valid',
    anomaly_flag BOOLEAN DEFAULT FALSE,
    CONSTRAINT uq_poi_meter_cleaned UNIQUE (plant_id, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_poi_meter_cleaned_timestamp ON silver.poi_meter_cleaned(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_poi_meter_cleaned_quality ON silver.poi_meter_cleaned(data_quality_flag);

-- Silver: Meteorological Station Cleaned Data
CREATE TABLE IF NOT EXISTS silver.meteo_station_cleaned (
    id BIGSERIAL PRIMARY KEY,
    plant_id UUID NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    amb_temp_C NUMERIC(6, 2),
    module_temp_C NUMERIC(6, 2),
    wind_speed_ms NUMERIC(8, 2),
    wind_dir_deg NUMERIC(6, 2),
    humidity_percent NUMERIC(5, 2),
    poa_irradiance_wm2 NUMERIC(10, 2),
    source_bronze_id BIGINT REFERENCES bronze.meteo_station_raw(id),
    processed_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    data_quality_flag VARCHAR(50) DEFAULT 'valid',
    anomaly_flag BOOLEAN DEFAULT FALSE,
    CONSTRAINT uq_meteo_cleaned UNIQUE (plant_id, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_meteo_cleaned_timestamp ON silver.meteo_station_cleaned(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_meteo_cleaned_quality ON silver.meteo_station_cleaned(data_quality_flag);

-- Silver: System Status Cleaned Data
CREATE TABLE IF NOT EXISTS silver.system_status_cleaned (
    id BIGSERIAL PRIMARY KEY,
    plant_id UUID NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    total_failures INTEGER,
    stress_level NUMERIC(5, 3),
    affected_components INTEGER,
    critical_failures INTEGER,
    total_components INTEGER,
    healthy_components INTEGER,
    failed_components INTEGER,
    source_bronze_id BIGINT REFERENCES bronze.system_status_raw(id),
    processed_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    data_quality_flag VARCHAR(50) DEFAULT 'valid',
    anomaly_flag BOOLEAN DEFAULT FALSE,
    CONSTRAINT uq_system_status_cleaned UNIQUE (plant_id, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_system_status_cleaned_timestamp ON silver.system_status_cleaned(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_system_status_cleaned_quality ON silver.system_status_cleaned(data_quality_flag);