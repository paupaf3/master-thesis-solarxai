-- ============================================================================
-- GOLD LAYER - Daily Aggregations, Hourly Aggregations and Summaries
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS gold;

-- Gold: Inverter Daily Aggregations
CREATE TABLE IF NOT EXISTS gold.inverter_daily (
    id BIGSERIAL PRIMARY KEY,
    plant_id UUID NOT NULL,
    inverter_id VARCHAR(50) NOT NULL,
    date DATE NOT NULL,
    
    -- Energy Production (integrated power over time)
    total_energy_kwh NUMERIC(12, 3),
    dc_energy_kwh NUMERIC(12, 3),
    
    -- Power Statistics
    peak_power_kw NUMERIC(10, 3),
    avg_power_kw NUMERIC(10, 3),
    min_power_kw NUMERIC(10, 3),
    
    -- DC Statistics
    avg_dc_voltage_v NUMERIC(8, 2),
    avg_dc_current_a NUMERIC(8, 2),
    
    -- Efficiency Metrics
    avg_efficiency NUMERIC(5, 4),
    min_efficiency NUMERIC(5, 4),
    efficiency_below_threshold_minutes INTEGER,
    
    -- Temperature Statistics
    temp_avg_c NUMERIC(6, 2),
    temp_min_c NUMERIC(6, 2),
    temp_max_c NUMERIC(6, 2),
    
    -- Frequency Statistics
    freq_avg_hz NUMERIC(10, 2),
    freq_min_hz NUMERIC(10, 2),
    freq_max_hz NUMERIC(10, 2),
    
    -- Availability & Operation
    availability_percent NUMERIC(5, 2),
    operating_hours NUMERIC(5, 2),
    total_records INTEGER,
    valid_records INTEGER,
    
    -- Failures
    total_failures INTEGER DEFAULT 0,
    failure_types_summary JSONB,
    string_failure_events INTEGER,
    
    -- Metadata
    processed_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT uq_inverter_daily UNIQUE (plant_id, inverter_id, date)
);

CREATE INDEX IF NOT EXISTS idx_inverter_daily_plant_id ON gold.inverter_daily(plant_id);
CREATE INDEX IF NOT EXISTS idx_inverter_daily_inverter_id ON gold.inverter_daily(inverter_id);
CREATE INDEX IF NOT EXISTS idx_inverter_daily_date ON gold.inverter_daily(date DESC);
CREATE INDEX IF NOT EXISTS idx_inverter_daily_plant_date ON gold.inverter_daily(plant_id, date DESC);

-- Gold: Meteorological Station Daily Aggregations
CREATE TABLE IF NOT EXISTS gold.meteo_station_daily (
    id BIGSERIAL PRIMARY KEY,
    plant_id UUID NOT NULL,
    date DATE NOT NULL,
    
    -- Temperature Statistics (Ambient)
    temp_avg_c NUMERIC(6, 2),
    temp_min_c NUMERIC(6, 2),
    temp_max_c NUMERIC(6, 2),
    
    -- Module Temperature Statistics
    module_temp_avg_c NUMERIC(6, 2),
    module_temp_min_c NUMERIC(6, 2),
    module_temp_max_c NUMERIC(6, 2),
    
    -- Humidity Statistics
    humidity_avg_percent NUMERIC(5, 2),
    humidity_min_percent NUMERIC(5, 2),
    humidity_max_percent NUMERIC(5, 2),
    
    -- Wind Statistics
    wind_speed_avg_ms NUMERIC(8, 2),
    wind_speed_max_ms NUMERIC(8, 2),
    predominant_wind_dir_deg NUMERIC(6, 2),
    
    -- Irradiance & Irradiation
    total_irradiation_kwh_m2 NUMERIC(10, 4),  -- Integrated irradiance (energy)
    peak_irradiance_wm2 NUMERIC(10, 2),
    avg_irradiance_wm2 NUMERIC(10, 2),
    sunshine_hours NUMERIC(5, 2),              -- Hours with irradiance > 120 W/m²
    
    -- Data Quality
    total_records INTEGER,
    valid_records INTEGER,
    
    -- Metadata
    processed_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT uq_meteo_daily UNIQUE (plant_id, date)
);

CREATE INDEX IF NOT EXISTS idx_meteo_daily_plant_id ON gold.meteo_station_daily(plant_id);
CREATE INDEX IF NOT EXISTS idx_meteo_daily_date ON gold.meteo_station_daily(date DESC);
CREATE INDEX IF NOT EXISTS idx_meteo_daily_plant_date ON gold.meteo_station_daily(plant_id, date DESC);

-- Gold: POI Meter Daily Aggregations
CREATE TABLE IF NOT EXISTS gold.poi_meter_daily (
    id BIGSERIAL PRIMARY KEY,
    plant_id UUID NOT NULL,
    date DATE NOT NULL,
    
    -- Energy Statistics
    total_export_kwh NUMERIC(14, 3),
    total_import_kwh NUMERIC(14, 3),
    
    -- Power Statistics
    peak_export_kw NUMERIC(12, 2),
    peak_import_kw NUMERIC(12, 2),
    avg_export_kw NUMERIC(12, 2),
    avg_import_kw NUMERIC(12, 2),
    
    -- Reactive Power
    total_reactive_kvarh NUMERIC(14, 3),
    avg_reactive_kvar NUMERIC(12, 2),
    
    -- Grid Voltage Statistics
    voltage_l1_avg_v NUMERIC(8, 2),
    voltage_l2_avg_v NUMERIC(8, 2),
    voltage_l3_avg_v NUMERIC(8, 2),
    voltage_imbalance_pct NUMERIC(5, 2),
    voltage_deviation_minutes INTEGER,
    
    -- Frequency Statistics
    frequency_avg_hz NUMERIC(6, 3),
    frequency_min_hz NUMERIC(6, 3),
    frequency_max_hz NUMERIC(6, 3),
    frequency_deviation_minutes INTEGER,
    
    -- Power Factor Statistics
    power_factor_avg NUMERIC(5, 3),
    power_factor_min NUMERIC(5, 3),
    power_factor_max NUMERIC(5, 3),
    
    -- Availability
    availability_percent NUMERIC(5, 2),
    connection_issue_hours NUMERIC(5, 2),
    
    -- Failures
    total_failures INTEGER DEFAULT 0,
    
    -- Data Quality
    total_records INTEGER,
    valid_records INTEGER,
    
    -- Metadata
    processed_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT uq_poi_meter_daily UNIQUE (plant_id, date)
);

CREATE INDEX IF NOT EXISTS idx_poi_meter_daily_plant_id ON gold.poi_meter_daily(plant_id);
CREATE INDEX IF NOT EXISTS idx_poi_meter_daily_date ON gold.poi_meter_daily(date DESC);
CREATE INDEX IF NOT EXISTS idx_poi_meter_daily_plant_date ON gold.poi_meter_daily(plant_id, date DESC);

-- Gold: System Status Daily Aggregations
CREATE TABLE IF NOT EXISTS gold.system_status_daily (
    id BIGSERIAL PRIMARY KEY,
    plant_id UUID NOT NULL,
    date DATE NOT NULL,
    
    -- Failure Statistics
    total_failures_sum INTEGER,
    max_concurrent_failures INTEGER,
    avg_failures NUMERIC(8, 2),
    
    -- Stress Level Statistics
    stress_level_avg NUMERIC(5, 3),
    stress_level_max NUMERIC(5, 3),
    stress_level_min NUMERIC(5, 3),
    
    -- Component Health
    avg_healthy_components NUMERIC(8, 2),
    min_healthy_components INTEGER,
    avg_failed_components NUMERIC(8, 2),
    max_failed_components INTEGER,
    
    -- Critical Failures
    total_critical_failures INTEGER,
    max_concurrent_critical INTEGER,
    
    -- Data Quality
    total_records INTEGER,
    valid_records INTEGER,
    
    -- Metadata
    processed_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT uq_system_status_daily UNIQUE (plant_id, date)
);

CREATE INDEX IF NOT EXISTS idx_system_status_daily_plant_id ON gold.system_status_daily(plant_id);
CREATE INDEX IF NOT EXISTS idx_system_status_daily_date ON gold.system_status_daily(date DESC);
CREATE INDEX IF NOT EXISTS idx_system_status_daily_plant_date ON gold.system_status_daily(plant_id, date DESC);


-- Gold: Plant Daily Summary (Cross-component aggregation)
CREATE TABLE IF NOT EXISTS gold.plant_daily_summary (
    id BIGSERIAL PRIMARY KEY,
    plant_id UUID NOT NULL,
    date DATE NOT NULL,
    
    -- Energy Balance
    total_generation_kwh NUMERIC(14, 3),       -- Sum of all inverters
    total_consumption_kwh NUMERIC(14, 3),      -- From POI meter import
    net_energy_kwh NUMERIC(14, 3),             -- Generation - Consumption
    
    -- Self-Consumption Metrics
    self_consumption_kwh NUMERIC(14, 3),       -- MIN(generation, consumption)
    self_consumption_ratio NUMERIC(5, 4),      -- self_consumption / generation
    grid_export_kwh NUMERIC(14, 3),            -- Excess sent to grid
    grid_import_kwh NUMERIC(14, 3),            -- Energy bought from grid
    
    -- Solar Resource
    total_irradiation_kwh_m2 NUMERIC(10, 4),   -- From meteo
    sunshine_hours NUMERIC(5, 2),
    
    -- Performance KPIs
    specific_yield_kwh_kwp NUMERIC(8, 4),      -- Energy per installed kWp (needs plant capacity)
    performance_ratio NUMERIC(5, 4),           -- Actual vs theoretical
    capacity_factor NUMERIC(5, 4),             -- Actual / (capacity * 24h)
    
    -- Temperature Impact
    avg_ambient_temp_c NUMERIC(6, 2),
    avg_module_temp_c NUMERIC(6, 2),
    temp_loss_factor NUMERIC(5, 4),            -- Temperature-related losses
    
    -- Availability & Reliability
    plant_availability_percent NUMERIC(5, 2),
    inverter_availability_avg NUMERIC(5, 2),
    
    -- Failures Summary
    total_failures INTEGER DEFAULT 0,
    critical_failures INTEGER DEFAULT 0,
    inverter_failures INTEGER DEFAULT 0,
    system_stress_avg NUMERIC(5, 3),
    
    -- Inverter Fleet Summary
    active_inverters INTEGER,
    total_inverters INTEGER,
    
    -- Data Quality
    data_completeness_percent NUMERIC(5, 2),
    
    -- Metadata
    processed_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT uq_plant_daily_summary UNIQUE (plant_id, date)
);

CREATE INDEX IF NOT EXISTS idx_plant_daily_plant_id ON gold.plant_daily_summary(plant_id);
CREATE INDEX IF NOT EXISTS idx_plant_daily_date ON gold.plant_daily_summary(date DESC);
CREATE INDEX IF NOT EXISTS idx_plant_daily_plant_date ON gold.plant_daily_summary(plant_id, date DESC);
CREATE INDEX IF NOT EXISTS idx_plant_daily_performance ON gold.plant_daily_summary(performance_ratio);


-- Gold: Failure Daily Summary (Cross-component failure analysis)
CREATE TABLE IF NOT EXISTS gold.failure_daily_summary (
    id BIGSERIAL PRIMARY KEY,
    plant_id UUID NOT NULL,
    date DATE NOT NULL,
    component_type VARCHAR(50) NOT NULL,       -- 'inverter', 'meteo', 'poi_meter', 'system'
    component_id VARCHAR(50),                   -- Specific component ID (NULL for system-wide)
    
    -- Failure Counts
    failure_count INTEGER DEFAULT 0,
    critical_failure_count INTEGER DEFAULT 0,
    
    -- Failure Types Breakdown
    failure_types JSONB,
    
    -- Duration & Impact
    total_downtime_minutes NUMERIC(10, 2),
    estimated_energy_loss_kwh NUMERIC(12, 3),
    
    -- MTBF/MTTR (if calculable)
    mean_time_between_failures_hours NUMERIC(10, 2),
    mean_time_to_repair_minutes NUMERIC(10, 2),
    
    -- Metadata
    processed_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT uq_failure_daily UNIQUE (plant_id, date, component_type, component_id)
);

CREATE INDEX IF NOT EXISTS idx_failure_daily_plant_id ON gold.failure_daily_summary(plant_id);
CREATE INDEX IF NOT EXISTS idx_failure_daily_date ON gold.failure_daily_summary(date DESC);
CREATE INDEX IF NOT EXISTS idx_failure_daily_component ON gold.failure_daily_summary(component_type);
CREATE INDEX IF NOT EXISTS idx_failure_daily_plant_date ON gold.failure_daily_summary(plant_id, date DESC);

-- Gold: Inverter Hourly Aggregations
CREATE TABLE IF NOT EXISTS gold.inverter_hourly (
    id BIGSERIAL PRIMARY KEY,
    plant_id UUID NOT NULL,
    inverter_id VARCHAR(50) NOT NULL,
    timestamp_hour TIMESTAMP WITH TIME ZONE NOT NULL,  -- Truncated to hour
    
    -- Energy Production
    energy_kwh NUMERIC(10, 4),                 -- Integrated power over the hour
    
    -- Power Statistics
    avg_power_kw NUMERIC(10, 3),
    max_power_kw NUMERIC(10, 3),
    min_power_kw NUMERIC(10, 3),
    
    -- Temperature Statistics
    temp_avg_c NUMERIC(6, 2),
    temp_min_c NUMERIC(6, 2),
    temp_max_c NUMERIC(6, 2),
    
    -- Frequency Statistics
    freq_avg_hz NUMERIC(10, 2),
    
    -- Availability
    availability_percent NUMERIC(5, 2),
    operating_minutes INTEGER,
    
    -- Failures
    failure_count INTEGER DEFAULT 0,
    failure_types JSONB,
    
    -- Data Quality
    record_count INTEGER,
    valid_record_count INTEGER,
    
    -- Metadata
    processed_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT uq_inverter_hourly UNIQUE (plant_id, inverter_id, timestamp_hour)
);

CREATE INDEX IF NOT EXISTS idx_inverter_hourly_plant_id ON gold.inverter_hourly(plant_id);
CREATE INDEX IF NOT EXISTS idx_inverter_hourly_inverter_id ON gold.inverter_hourly(inverter_id);
CREATE INDEX IF NOT EXISTS idx_inverter_hourly_timestamp ON gold.inverter_hourly(timestamp_hour DESC);
CREATE INDEX IF NOT EXISTS idx_inverter_hourly_plant_ts ON gold.inverter_hourly(plant_id, timestamp_hour DESC);

-- Gold: Meteorological Station Hourly Aggregations
CREATE TABLE IF NOT EXISTS gold.meteo_station_hourly (
    id BIGSERIAL PRIMARY KEY,
    plant_id UUID NOT NULL,
    timestamp_hour TIMESTAMP WITH TIME ZONE NOT NULL,
    
    -- Temperature (Ambient)
    temp_avg_c NUMERIC(6, 2),
    temp_min_c NUMERIC(6, 2),
    temp_max_c NUMERIC(6, 2),
    
    -- Module Temperature
    module_temp_avg_c NUMERIC(6, 2),
    module_temp_min_c NUMERIC(6, 2),
    module_temp_max_c NUMERIC(6, 2),
    
    -- Humidity
    humidity_avg_percent NUMERIC(5, 2),
    humidity_min_percent NUMERIC(5, 2),
    humidity_max_percent NUMERIC(5, 2),
    
    -- Wind
    wind_speed_avg_ms NUMERIC(8, 2),
    wind_speed_max_ms NUMERIC(8, 2),
    wind_dir_avg_deg NUMERIC(6, 2),
    
    -- Irradiance & Irradiation
    irradiation_wh_m2 NUMERIC(10, 3),          -- Integrated irradiance for the hour
    avg_irradiance_wm2 NUMERIC(10, 2),
    peak_irradiance_wm2 NUMERIC(10, 2),
    
    -- Data Quality
    record_count INTEGER,
    valid_record_count INTEGER,
    
    -- Metadata
    processed_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT uq_meteo_hourly UNIQUE (plant_id, timestamp_hour)
);

CREATE INDEX IF NOT EXISTS idx_meteo_hourly_plant_id ON gold.meteo_station_hourly(plant_id);
CREATE INDEX IF NOT EXISTS idx_meteo_hourly_timestamp ON gold.meteo_station_hourly(timestamp_hour DESC);
CREATE INDEX IF NOT EXISTS idx_meteo_hourly_plant_ts ON gold.meteo_station_hourly(plant_id, timestamp_hour DESC);

-- Gold: POI Meter Hourly Aggregations
CREATE TABLE IF NOT EXISTS gold.poi_meter_hourly (
    id BIGSERIAL PRIMARY KEY,
    plant_id UUID NOT NULL,
    timestamp_hour TIMESTAMP WITH TIME ZONE NOT NULL,
    
    -- Energy Statistics
    export_energy_kwh NUMERIC(12, 4),
    import_energy_kwh NUMERIC(12, 4),
    reactive_kvarh NUMERIC(12, 4),
    
    -- Power Statistics
    avg_export_kw NUMERIC(12, 2),
    max_export_kw NUMERIC(12, 2),
    avg_import_kw NUMERIC(12, 2),
    max_import_kw NUMERIC(12, 2),
    avg_reactive_kvar NUMERIC(12, 2),
    
    -- Grid Voltage Statistics (3-phase)
    voltage_l1_avg_v NUMERIC(8, 2),
    voltage_l2_avg_v NUMERIC(8, 2),
    voltage_l3_avg_v NUMERIC(8, 2),
    voltage_imbalance_pct NUMERIC(5, 2),
    
    -- Grid Frequency Statistics
    frequency_avg_hz NUMERIC(6, 3),
    frequency_min_hz NUMERIC(6, 3),
    frequency_max_hz NUMERIC(6, 3),
    frequency_deviation_count INTEGER DEFAULT 0,
    
    -- Power Factor
    power_factor_avg NUMERIC(5, 3),
    power_factor_min NUMERIC(5, 3),
    
    -- Availability
    availability_percent NUMERIC(5, 2),
    connection_issue_minutes INTEGER DEFAULT 0,
    
    -- Failures
    failure_count INTEGER DEFAULT 0,
    
    -- Data Quality
    record_count INTEGER,
    valid_record_count INTEGER,
    
    -- Metadata
    processed_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT uq_poi_meter_hourly UNIQUE (plant_id, timestamp_hour)
);

CREATE INDEX IF NOT EXISTS idx_poi_meter_hourly_plant_id ON gold.poi_meter_hourly(plant_id);
CREATE INDEX IF NOT EXISTS idx_poi_meter_hourly_timestamp ON gold.poi_meter_hourly(timestamp_hour DESC);
CREATE INDEX IF NOT EXISTS idx_poi_meter_hourly_plant_ts ON gold.poi_meter_hourly(plant_id, timestamp_hour DESC);

-- Gold: System Status Hourly Aggregations
CREATE TABLE IF NOT EXISTS gold.system_status_hourly (
    id BIGSERIAL PRIMARY KEY,
    plant_id UUID NOT NULL,
    timestamp_hour TIMESTAMP WITH TIME ZONE NOT NULL,
    
    -- Failure Statistics
    total_failures INTEGER,
    max_concurrent_failures INTEGER,
    avg_failures NUMERIC(8, 2),
    
    -- Stress Level Statistics
    stress_level_avg NUMERIC(5, 3),
    stress_level_max NUMERIC(5, 3),
    stress_level_min NUMERIC(5, 3),
    
    -- Component Health
    avg_healthy_components NUMERIC(8, 2),
    min_healthy_components INTEGER,
    avg_failed_components NUMERIC(8, 2),
    max_failed_components INTEGER,
    
    -- Critical Failures
    critical_failure_count INTEGER DEFAULT 0,
    max_concurrent_critical INTEGER DEFAULT 0,
    
    -- Data Quality
    record_count INTEGER,
    valid_record_count INTEGER,
    
    -- Metadata
    processed_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT uq_system_status_hourly UNIQUE (plant_id, timestamp_hour)
);

CREATE INDEX IF NOT EXISTS idx_system_status_hourly_plant_id ON gold.system_status_hourly(plant_id);
CREATE INDEX IF NOT EXISTS idx_system_status_hourly_timestamp ON gold.system_status_hourly(timestamp_hour DESC);
CREATE INDEX IF NOT EXISTS idx_system_status_hourly_plant_ts ON gold.system_status_hourly(plant_id, timestamp_hour DESC);

-- Gold: Plant Hourly Summary (Real-time KPIs)
CREATE TABLE IF NOT EXISTS gold.plant_hourly_summary (
    id BIGSERIAL PRIMARY KEY,
    plant_id UUID NOT NULL,
    timestamp_hour TIMESTAMP WITH TIME ZONE NOT NULL,
    
    -- Energy Balance
    total_generation_kwh NUMERIC(12, 4),
    total_consumption_kwh NUMERIC(12, 4),
    net_energy_kwh NUMERIC(12, 4),
    
    -- Self-Consumption
    self_consumption_kwh NUMERIC(12, 4),
    grid_export_kwh NUMERIC(12, 4),
    grid_import_kwh NUMERIC(12, 4),
    
    -- Power
    total_power_kw NUMERIC(12, 3),
    import_kw NUMERIC(12, 3),
    
    -- Solar Resource
    irradiation_wh_m2 NUMERIC(10, 3),
    avg_irradiance_wm2 NUMERIC(10, 2),
    
    -- Performance
    instantaneous_pr NUMERIC(5, 4),            -- Performance ratio for the hour
    
    -- Environmental
    avg_ambient_temp_c NUMERIC(6, 2),
    avg_module_temp_c NUMERIC(6, 2),
    
    -- System Health
    active_inverters INTEGER,
    total_inverters INTEGER,
    failure_count INTEGER DEFAULT 0,
    system_stress_avg NUMERIC(5, 3),
    
    -- Data Quality
    data_completeness_percent NUMERIC(5, 2),
    
    -- Metadata
    processed_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT uq_plant_hourly UNIQUE (plant_id, timestamp_hour)
);

CREATE INDEX IF NOT EXISTS idx_plant_hourly_plant_id ON gold.plant_hourly_summary(plant_id);
CREATE INDEX IF NOT EXISTS idx_plant_hourly_timestamp ON gold.plant_hourly_summary(timestamp_hour DESC);
CREATE INDEX IF NOT EXISTS idx_plant_hourly_plant_ts ON gold.plant_hourly_summary(plant_id, timestamp_hour DESC);