import pandas as pd
from sqlalchemy import create_engine


def _make_engine(cfg):
    url = f"postgresql+psycopg2://{cfg.db_user}:{cfg.db_password}@{cfg.db_host}:{cfg.db_port}/{cfg.db_name}"
    return create_engine(url)


def get_last_forecast_ts(engine, inverter_id):
    """Get the last forecasted timestamp for this inverter."""
    query = """
        SELECT MAX(timestamp) as last_ts
        FROM gold.forecast_predictions
        WHERE inverter_id = %(inv)s
    """
    result = pd.read_sql(query, engine, params={"inv": inverter_id})
    if result.empty or pd.isna(result.iloc[0]['last_ts']):
        return None
    return result.iloc[0]['last_ts']


def get_last_data_ts(engine, inverter_id):
    """Get the latest timestamp available in silver data."""
    query = """
        SELECT MAX(timestamp) as last_ts
        FROM silver.inverter_cleaned
        WHERE inverter_id = %(inv)s
    """
    result = pd.read_sql(query, engine, params={"inv": inverter_id})
    if result.empty or pd.isna(result.iloc[0]['last_ts']):
        return None
    return result.iloc[0]['last_ts']


def get_plant_id(engine, inverter_id):
    """Get the plant_id for this inverter from actual data."""
    query = """
        SELECT DISTINCT plant_id
        FROM silver.inverter_cleaned
        WHERE inverter_id = %(inv)s
        LIMIT 1
    """
    result = pd.read_sql(query, engine, params={"inv": inverter_id})
    if result.empty:
        raise ValueError(f"Inverter {inverter_id} not found in inverter_cleaned table")
    return str(result.iloc[0]['plant_id'])


def generate_future_timestamps(cfg, inverter_id):
    """
    Generate future timestamps for forecasting.
    
    Logic:
    - Always anchor predictions to last_data_ts (the latest real inverter data)
    - Generate timestamps for the next `forecast_horizon_hours` hours from last_data_ts
    - Skip if we've already forecasted the full horizon from current data
    - Use `forecast_frequency_minutes` as the step size
    
    Returns DataFrame with: plant_id, inverter_id, timestamp
    """
    engine = _make_engine(cfg)
    
    plant_id = get_plant_id(engine, inverter_id)
    last_forecast_ts = get_last_forecast_ts(engine, inverter_id)
    last_data_ts = get_last_data_ts(engine, inverter_id)
    
    engine.dispose()
    
    # Determine starting point - always anchor to actual data, not predictions
    if last_data_ts is None:
        raise ValueError(f"No data found for inverter {inverter_id} in silver.inverter_cleaned")
    
    # Convert last_data_ts to UTC timestamp
    data_ts = pd.Timestamp(last_data_ts)
    data_ts = data_ts.tz_convert('UTC') if data_ts.tzinfo else data_ts.tz_localize('UTC')
    
    # Calculate the target end of forecast window (always relative to actual data)
    freq = f"{cfg.forecast_frequency_minutes}min"
    horizon = pd.Timedelta(hours=cfg.forecast_horizon_hours)
    target_end_ts = data_ts + horizon
    
    # Check if we've already forecasted the full window from current data
    if last_forecast_ts is not None:
        forecast_ts = pd.Timestamp(last_forecast_ts)
        forecast_ts = forecast_ts.tz_convert('UTC') if forecast_ts.tzinfo else forecast_ts.tz_localize('UTC')
        
        if forecast_ts >= target_end_ts:
            # Already have forecasts covering the full horizon from last data
            return pd.DataFrame(columns=['plant_id', 'inverter_id', 'timestamp'])
        
        # Start from where we left off (but never before data_ts)
        start_ts = max(forecast_ts, data_ts)
    else:
        start_ts = data_ts
    
    end_ts = target_end_ts
    
    # Create date range (exclusive start, inclusive end)
    # start_ts already has UTC timezone, no need to pass tz parameter
    future_timestamps = pd.date_range(
        start=start_ts + pd.Timedelta(minutes=cfg.forecast_frequency_minutes),
        end=end_ts,
        freq=freq
    )
    
    if len(future_timestamps) == 0:
        return pd.DataFrame(columns=['plant_id', 'inverter_id', 'timestamp'])
    
    df = pd.DataFrame({
        'plant_id': plant_id,
        'inverter_id': inverter_id,
        'timestamp': future_timestamps,
    })
    
    return df