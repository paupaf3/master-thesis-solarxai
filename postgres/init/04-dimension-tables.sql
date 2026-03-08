-- ============================================================================
-- DIMENSION TABLES - Asset Normalization and Configuration
-- ============================================================================
-- These tables store static configuration and asset specifications that
-- are used to calculate relative performance metrics (e.g., load factor,
-- efficiency vs nominal, capacity utilization).
--
-- Data is seeded from simulator/config/plant_config.json
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS silver;

-- ============================================================================
-- Plant Configuration Dimension
-- ============================================================================
-- Stores plant-level configuration and nominal values

CREATE TABLE IF NOT EXISTS silver.dim_plant_config (
    id SERIAL PRIMARY KEY,
    plant_id UUID NOT NULL UNIQUE,
    plant_name VARCHAR(255) NOT NULL,
    
    -- Location
    latitude NUMERIC(10, 6),
    longitude NUMERIC(10, 6),
    timezone VARCHAR(50),
    altitude_m NUMERIC(8, 2),
    
    -- Capacity
    total_capacity_kwp NUMERIC(12, 3) NOT NULL,
    total_surface_m2 NUMERIC(12, 2) NOT NULL,
    total_inverters INTEGER NOT NULL,
    
    -- Grid Connection
    grid_voltage_nominal_v NUMERIC(8, 2),
    grid_frequency_nominal_hz NUMERIC(6, 2),
    grid_phases INTEGER,
    
    -- Dates
    commissioning_date DATE,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    config_version VARCHAR(20)
);

CREATE INDEX IF NOT EXISTS idx_dim_plant_config_plant_id ON silver.dim_plant_config(plant_id);

-- ============================================================================
-- Asset Dimension
-- ============================================================================
-- Stores specifications for each asset (inverters, meters, sensors)

CREATE TABLE IF NOT EXISTS silver.dim_asset (
    id SERIAL PRIMARY KEY,
    plant_id UUID NOT NULL REFERENCES silver.dim_plant_config(plant_id),
    asset_id VARCHAR(50) NOT NULL,
    asset_type VARCHAR(50) NOT NULL,  -- 'inverter', 'poi_meter', 'meteo_station'
    asset_group VARCHAR(50),          -- Group identifier (A, B, C for inverters)
    
    -- Capacity (applicable to inverters and meters)
    nominal_capacity_kw NUMERIC(10, 3),
    panel_surface_m2 NUMERIC(10, 2),
    
    -- DC Side Specifications (inverters only)
    dc_voltage_mpp_min_v NUMERIC(8, 2),
    dc_voltage_mpp_max_v NUMERIC(8, 2),
    dc_voltage_nominal_v NUMERIC(8, 2),
    dc_current_max_a NUMERIC(8, 2),
    strings_count INTEGER,
    panels_per_string INTEGER,
    panel_wp NUMERIC(8, 2),
    
    -- AC Side Specifications
    ac_voltage_nominal_v NUMERIC(8, 2),
    ac_frequency_nominal_hz NUMERIC(6, 2),
    power_factor_nominal NUMERIC(4, 3),
    
    -- Efficiency Specifications
    efficiency_dc_ac_nominal NUMERIC(5, 4),
    efficiency_dc_ac_min_acceptable NUMERIC(5, 4),
    efficiency_system_min NUMERIC(5, 4),
    efficiency_system_max NUMERIC(5, 4),
    
    -- Thermal Specifications
    temp_coefficient_pct_per_c NUMERIC(6, 4),
    stc_temperature_c NUMERIC(5, 2),
    max_operating_temp_c NUMERIC(5, 2),
    
    -- POI Meter Specific
    max_export_kw NUMERIC(10, 3),
    max_import_kw NUMERIC(10, 3),
    meter_type VARCHAR(50),  -- 'bidirectional', 'export_only', 'import_only'
    
    -- Status
    is_active BOOLEAN DEFAULT TRUE,
    commissioned_date DATE,
    decommissioned_date DATE,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT uq_asset UNIQUE (plant_id, asset_id)
);

CREATE INDEX IF NOT EXISTS idx_dim_asset_plant_id ON silver.dim_asset(plant_id);
CREATE INDEX IF NOT EXISTS idx_dim_asset_asset_id ON silver.dim_asset(asset_id);
CREATE INDEX IF NOT EXISTS idx_dim_asset_asset_type ON silver.dim_asset(asset_type);

-- ============================================================================
-- Thresholds Configuration
-- ============================================================================
-- Stores alert and warning thresholds for monitoring

CREATE TABLE IF NOT EXISTS silver.dim_thresholds (
    id SERIAL PRIMARY KEY,
    plant_id UUID NOT NULL REFERENCES silver.dim_plant_config(plant_id),
    threshold_category VARCHAR(50) NOT NULL,  -- 'inverter', 'dc_diagnostics', 'poi_meter'
    threshold_name VARCHAR(100) NOT NULL,
    threshold_value NUMERIC(12, 4) NOT NULL,
    threshold_unit VARCHAR(20),
    severity VARCHAR(20),  -- 'warning', 'alert', 'critical'
    description TEXT,
    
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT uq_threshold UNIQUE (plant_id, threshold_category, threshold_name)
);

CREATE INDEX IF NOT EXISTS idx_dim_thresholds_plant_id ON silver.dim_thresholds(plant_id);
CREATE INDEX IF NOT EXISTS idx_dim_thresholds_category ON silver.dim_thresholds(threshold_category);

-- ============================================================================
-- Trigger for updated_at timestamps
-- ============================================================================

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

DROP TRIGGER IF EXISTS update_dim_plant_config_updated_at ON silver.dim_plant_config;
CREATE TRIGGER update_dim_plant_config_updated_at
    BEFORE UPDATE ON silver.dim_plant_config
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_dim_asset_updated_at ON silver.dim_asset;
CREATE TRIGGER update_dim_asset_updated_at
    BEFORE UPDATE ON silver.dim_asset
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_dim_thresholds_updated_at ON silver.dim_thresholds;
CREATE TRIGGER update_dim_thresholds_updated_at
    BEFORE UPDATE ON silver.dim_thresholds
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
