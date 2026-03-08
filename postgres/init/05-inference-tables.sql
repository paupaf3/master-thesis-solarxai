-- ============================================================================
-- GOLD LAYER - Inference Engine Output Tables
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS gold;

-- Anomaly Detection Predictions
CREATE TABLE IF NOT EXISTS gold.anomaly_predictions (
    id BIGSERIAL PRIMARY KEY,
    plant_id UUID NOT NULL,
    inverter_id VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    model_run_id VARCHAR(64) NOT NULL,
    reconstruction_error NUMERIC(12, 6) NOT NULL,
    threshold NUMERIC(12, 6) NOT NULL,
    is_anomaly SMALLINT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_anomaly_prediction UNIQUE (plant_id, inverter_id, timestamp, model_run_id)
);

CREATE INDEX IF NOT EXISTS idx_anomaly_pred_inverter ON gold.anomaly_predictions(inverter_id);
CREATE INDEX IF NOT EXISTS idx_anomaly_pred_timestamp ON gold.anomaly_predictions(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_anomaly_pred_anomaly ON gold.anomaly_predictions(is_anomaly);
CREATE INDEX IF NOT EXISTS idx_anomaly_pred_plant_ts ON gold.anomaly_predictions(plant_id, inverter_id, timestamp DESC);

-- Forecasting Predictions
CREATE TABLE IF NOT EXISTS gold.forecast_predictions (
    id BIGSERIAL PRIMARY KEY,
    plant_id UUID NOT NULL,
    inverter_id VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    model_run_id VARCHAR(64) NOT NULL,
    predicted_ac_power_kw NUMERIC(10, 3) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_forecast_prediction UNIQUE (plant_id, inverter_id, timestamp, model_run_id)
);

CREATE INDEX IF NOT EXISTS idx_forecast_pred_inverter ON gold.forecast_predictions(inverter_id);
CREATE INDEX IF NOT EXISTS idx_forecast_pred_timestamp ON gold.forecast_predictions(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_forecast_pred_plant_ts ON gold.forecast_predictions(plant_id, inverter_id, timestamp DESC);

-- Inference Watermark (tracks last processed timestamp per inverter per mode)
CREATE TABLE IF NOT EXISTS gold.inference_watermark (
    id BIGSERIAL PRIMARY KEY,
    inverter_id VARCHAR(50) NOT NULL,
    inference_mode VARCHAR(20) NOT NULL,
    last_processed_ts TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_inference_watermark UNIQUE (inverter_id, inference_mode)
);

CREATE INDEX IF NOT EXISTS idx_watermark_inverter ON gold.inference_watermark(inverter_id);
CREATE INDEX IF NOT EXISTS idx_watermark_mode ON gold.inference_watermark(inference_mode);