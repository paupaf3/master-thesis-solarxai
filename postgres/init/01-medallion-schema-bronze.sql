-- Medallion Architecture Schema Creation for SolarX.ai
-- ============================================================================
-- BRONZE LAYER - Raw Data Storage
-- ============================================================================

-- Create bronze schema
CREATE SCHEMA IF NOT EXISTS bronze;

-- Bronze: Inverter Raw Data
CREATE TABLE IF NOT EXISTS bronze.inverter_raw (
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
    healthy_strings INTEGER,
    failed_strings INTEGER,
    active_failures INTEGER,
    failure_types JSONB,
    raw_json JSONB NOT NULL,
    ingestion_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    source VARCHAR(50) DEFAULT 'kafka',
    processing_status VARCHAR(50) DEFAULT 'new',
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_inverter_raw_inverter_id ON bronze.inverter_raw(inverter_id);
CREATE INDEX IF NOT EXISTS idx_inverter_raw_timestamp ON bronze.inverter_raw(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_inverter_raw_ingestion_timestamp ON bronze.inverter_raw(ingestion_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_inverter_raw_processing_status ON bronze.inverter_raw(processing_status);

-- Bronze: POI Meter Raw Data
CREATE TABLE IF NOT EXISTS bronze.poi_meter_raw (
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
    raw_json JSONB NOT NULL,
    ingestion_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    source VARCHAR(50) DEFAULT 'kafka',
    processing_status VARCHAR(50) DEFAULT 'new',
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_poi_meter_raw_timestamp ON bronze.poi_meter_raw(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_poi_meter_raw_ingestion_timestamp ON bronze.poi_meter_raw(ingestion_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_poi_meter_raw_processing_status ON bronze.poi_meter_raw(processing_status);

-- Bronze: Meteorological Data Raw Data
CREATE TABLE IF NOT EXISTS bronze.meteo_station_raw (
    id BIGSERIAL PRIMARY KEY,
    plant_id UUID NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    amb_temp_C NUMERIC(6, 2),
    module_temp_C NUMERIC(6, 2),
    wind_speed_ms NUMERIC(8, 2),
    wind_dir_deg NUMERIC(6, 2),
    humidity_percent NUMERIC(5, 2),
    poa_irradiance_wm2 NUMERIC(10, 2),
    raw_json JSONB NOT NULL,
    ingestion_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    source VARCHAR(50) DEFAULT 'kafka',
    processing_status VARCHAR(50) DEFAULT 'new',
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_meteo_raw_timestamp ON bronze.meteo_station_raw(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_meteo_raw_ingestion_timestamp ON bronze.meteo_station_raw(ingestion_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_meteo_raw_processing_status ON bronze.meteo_station_raw(processing_status);

-- Bronze: System Status Raw Data
CREATE TABLE IF NOT EXISTS bronze.system_status_raw (
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
    raw_json JSONB NOT NULL,
    ingestion_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    source VARCHAR(50) DEFAULT 'kafka',
    processing_status VARCHAR(50) DEFAULT 'new',
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_system_status_raw_timestamp ON bronze.system_status_raw(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_system_status_raw_ingestion_timestamp ON bronze.system_status_raw(ingestion_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_system_status_raw_processing_status ON bronze.system_status_raw(processing_status);