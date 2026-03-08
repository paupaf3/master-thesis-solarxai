
import os
from typing import Dict, Any, List, Optional
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
from contextlib import contextmanager


def get_db_config() -> Dict[str, Any]:
    """Get database configuration from environment variables"""
    return {
        "host": os.getenv("DB_HOST"),
        "port": int(os.getenv("DB_PORT")),
        "database": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD")
    }


@contextmanager
def get_db_connection():
    """Context manager for database connections"""
    config = get_db_config()
    conn = None
    try:
        conn = psycopg2.connect(**config)
        yield conn
    finally:
        if conn:
            conn.close()


# ============================================================================
# HOURLY DATA FUNCTIONS (for near real-time dashboard)
# ============================================================================

def get_inverter_hourly_data(
    start_datetime: datetime,
    end_datetime: datetime,
    plant_id: Optional[str] = None,
    inverter_id: Optional[str] = None
) -> pd.DataFrame:
    """
    Fetch hourly inverter data for the specified datetime range.
    
    Args:
        start_datetime: Start of the time range
        end_datetime: End of the time range
        plant_id: Optional plant UUID filter
        inverter_id: Optional inverter ID filter
        
    Returns:
        DataFrame with hourly inverter data
    """
    query = """
        SELECT 
            plant_id,
            inverter_id,
            timestamp_hour,
            energy_kwh,
            avg_power_kw,
            max_power_kw,
            min_power_kw,
            temp_avg_c,
            temp_min_c,
            temp_max_c,
            freq_avg_hz,
            availability_percent,
            operating_minutes,
            failure_count,
            failure_types,
            record_count,
            valid_record_count,
            processed_timestamp
        FROM gold.inverter_hourly
        WHERE timestamp_hour >= %s AND timestamp_hour <= %s
    """
    
    params = [start_datetime, end_datetime]
    
    if plant_id:
        query += " AND plant_id = %s"
        params.append(plant_id)
    
    if inverter_id:
        query += " AND inverter_id = %s"
        params.append(inverter_id)
    
    query += " ORDER BY timestamp_hour DESC, inverter_id"
    
    with get_db_connection() as conn:
        return pd.read_sql_query(query, conn, params=params)


def get_meteo_hourly_data(
    start_datetime: datetime,
    end_datetime: datetime,
    plant_id: Optional[str] = None
) -> pd.DataFrame:
    """
    Fetch hourly meteorological data for the specified datetime range.
    
    Args:
        start_datetime: Start of the time range
        end_datetime: End of the time range
        plant_id: Optional plant UUID filter
        
    Returns:
        DataFrame with hourly meteorological data
    """
    query = """
        SELECT 
            plant_id,
            timestamp_hour,
            temp_avg_c,
            temp_min_c,
            temp_max_c,
            module_temp_avg_c,
            module_temp_min_c,
            module_temp_max_c,
            humidity_avg_percent,
            humidity_min_percent,
            humidity_max_percent,
            wind_speed_avg_ms,
            wind_speed_max_ms,
            wind_dir_avg_deg,
            irradiation_wh_m2,
            avg_irradiance_wm2,
            peak_irradiance_wm2,
            record_count,
            valid_record_count,
            processed_timestamp
        FROM gold.meteo_station_hourly
        WHERE timestamp_hour >= %s AND timestamp_hour <= %s
    """
    
    params = [start_datetime, end_datetime]
    
    if plant_id:
        query += " AND plant_id = %s"
        params.append(plant_id)
    
    query += " ORDER BY timestamp_hour DESC"
    
    with get_db_connection() as conn:
        return pd.read_sql_query(query, conn, params=params)


def get_poi_meter_hourly_data(
    start_datetime: datetime,
    end_datetime: datetime,
    plant_id: Optional[str] = None
) -> pd.DataFrame:
    """
    Fetch hourly POI meter data for the specified datetime range.
    
    Args:
        start_datetime: Start of the time range
        end_datetime: End of the time range
        plant_id: Optional plant UUID filter
        
    Returns:
        DataFrame with hourly POI meter data
    """
    query = """
        SELECT 
            plant_id,
            timestamp_hour,
            export_energy_kwh,
            import_energy_kwh,
            reactive_kvarh,
            avg_export_kw,
            max_export_kw,
            avg_import_kw,
            max_import_kw,
            avg_reactive_kvar,
            voltage_l1_avg_v,
            voltage_l2_avg_v,
            voltage_l3_avg_v,
            voltage_imbalance_pct,
            frequency_avg_hz,
            power_factor_avg,
            power_factor_min,
            availability_percent,
            connection_issue_minutes,
            failure_count,
            record_count,
            valid_record_count,
            processed_timestamp
        FROM gold.poi_meter_hourly
        WHERE timestamp_hour >= %s AND timestamp_hour <= %s
    """
    
    params = [start_datetime, end_datetime]
    
    if plant_id:
        query += " AND plant_id = %s"
        params.append(plant_id)
    
    query += " ORDER BY timestamp_hour DESC"
    
    with get_db_connection() as conn:
        return pd.read_sql_query(query, conn, params=params)


def get_system_status_hourly_data(
    start_datetime: datetime,
    end_datetime: datetime,
    plant_id: Optional[str] = None
) -> pd.DataFrame:
    """
    Fetch hourly system status data for the specified datetime range.
    
    Args:
        start_datetime: Start of the time range
        end_datetime: End of the time range
        plant_id: Optional plant UUID filter
        
    Returns:
        DataFrame with hourly system status data
    """
    query = """
        SELECT 
            plant_id,
            timestamp_hour,
            total_failures,
            max_concurrent_failures,
            avg_failures,
            stress_level_avg,
            stress_level_max,
            stress_level_min,
            avg_healthy_components,
            min_healthy_components,
            avg_failed_components,
            max_failed_components,
            critical_failure_count,
            max_concurrent_critical,
            record_count,
            valid_record_count,
            processed_timestamp
        FROM gold.system_status_hourly
        WHERE timestamp_hour >= %s AND timestamp_hour <= %s
    """
    
    params = [start_datetime, end_datetime]
    
    if plant_id:
        query += " AND plant_id = %s"
        params.append(plant_id)
    
    query += " ORDER BY timestamp_hour DESC"
    
    with get_db_connection() as conn:
        return pd.read_sql_query(query, conn, params=params)


def get_plant_hourly_summary(
    start_datetime: datetime,
    end_datetime: datetime,
    plant_id: Optional[str] = None
) -> pd.DataFrame:
    """
    Fetch hourly plant summary data for the specified datetime range.
    This is the main aggregated view for real-time monitoring.
    
    Args:
        start_datetime: Start of the time range
        end_datetime: End of the time range
        plant_id: Optional plant UUID filter
        
    Returns:
        DataFrame with hourly plant summary data
    """
    query = """
        SELECT 
            plant_id,
            timestamp_hour,
            total_generation_kwh,
            total_consumption_kwh,
            net_energy_kwh,
            self_consumption_kwh,
            grid_export_kwh,
            grid_import_kwh,
            total_power_kw,
            import_kw,
            irradiation_wh_m2,
            avg_irradiance_wm2,
            instantaneous_pr,
            avg_ambient_temp_c,
            avg_module_temp_c,
            active_inverters,
            total_inverters,
            failure_count,
            system_stress_avg,
            data_completeness_percent,
            processed_timestamp
        FROM gold.plant_hourly_summary
        WHERE timestamp_hour >= %s AND timestamp_hour <= %s
    """
    
    params = [start_datetime, end_datetime]
    
    if plant_id:
        query += " AND plant_id = %s"
        params.append(plant_id)
    
    query += " ORDER BY timestamp_hour DESC"
    
    with get_db_connection() as conn:
        return pd.read_sql_query(query, conn, params=params)


# ============================================================================
# DAILY DATA FUNCTIONS
# ============================================================================

def get_inverter_daily_data(
    start_date: datetime,
    end_date: datetime,
    plant_id: Optional[str] = None,
    inverter_id: Optional[str] = None
) -> pd.DataFrame:
    """
    Fetch daily inverter data for the specified date range.
    
    Args:
        start_date: Start date
        end_date: End date
        plant_id: Optional plant UUID filter
        inverter_id: Optional inverter ID filter
        
    Returns:
        DataFrame with daily inverter data
    """
    query = """
        SELECT 
            plant_id,
            inverter_id,
            date,
            total_energy_kwh,
            peak_power_kw,
            avg_power_kw,
            min_power_kw,
            temp_avg_c,
            temp_min_c,
            temp_max_c,
            freq_avg_hz,
            freq_min_hz,
            freq_max_hz,
            availability_percent,
            operating_hours,
            total_records,
            valid_records,
            total_failures,
            failure_types_summary,
            processed_timestamp
        FROM gold.inverter_daily
        WHERE date >= %s AND date <= %s
    """
    
    params = [start_date.date(), end_date.date()]
    
    if plant_id:
        query += " AND plant_id = %s"
        params.append(plant_id)
    
    if inverter_id:
        query += " AND inverter_id = %s"
        params.append(inverter_id)
    
    query += " ORDER BY date DESC, inverter_id"
    
    with get_db_connection() as conn:
        return pd.read_sql_query(query, conn, params=params)


def get_meteo_daily_data(
    start_date: datetime,
    end_date: datetime,
    plant_id: Optional[str] = None
) -> pd.DataFrame:
    """
    Fetch daily meteorological data for the specified date range.
    
    Args:
        start_date: Start date
        end_date: End date
        plant_id: Optional plant UUID filter
        
    Returns:
        DataFrame with daily meteorological data
    """
    query = """
        SELECT 
            plant_id,
            date,
            temp_avg_c,
            temp_min_c,
            temp_max_c,
            module_temp_avg_c,
            module_temp_min_c,
            module_temp_max_c,
            humidity_avg_percent,
            humidity_min_percent,
            humidity_max_percent,
            wind_speed_avg_ms,
            wind_speed_max_ms,
            predominant_wind_dir_deg,
            total_irradiation_kwh_m2,
            peak_irradiance_wm2,
            avg_irradiance_wm2,
            sunshine_hours,
            total_records,
            valid_records,
            processed_timestamp
        FROM gold.meteo_station_daily
        WHERE date >= %s AND date <= %s
    """
    
    params = [start_date.date(), end_date.date()]
    
    if plant_id:
        query += " AND plant_id = %s"
        params.append(plant_id)
    
    query += " ORDER BY date DESC"
    
    with get_db_connection() as conn:
        return pd.read_sql_query(query, conn, params=params)


def get_poi_meter_daily_data(
    start_date: datetime,
    end_date: datetime,
    plant_id: Optional[str] = None
) -> pd.DataFrame:
    """
    Fetch daily POI meter data for the specified date range.
    
    Args:
        start_date: Start date
        end_date: End date
        plant_id: Optional plant UUID filter
        
    Returns:
        DataFrame with daily POI meter data
    """
    query = """
        SELECT 
            plant_id,
            date,
            total_export_kwh,
            total_import_kwh,
            peak_export_kw,
            peak_import_kw,
            avg_export_kw,
            avg_import_kw,
            total_reactive_kvarh,
            avg_reactive_kvar,
            voltage_l1_avg_v,
            voltage_l2_avg_v,
            voltage_l3_avg_v,
            voltage_imbalance_pct,
            frequency_avg_hz,
            power_factor_avg,
            power_factor_min,
            power_factor_max,
            availability_percent,
            connection_issue_hours,
            total_failures,
            total_records,
            valid_records,
            processed_timestamp
        FROM gold.poi_meter_daily
        WHERE date >= %s AND date <= %s
    """
    
    params = [start_date.date(), end_date.date()]
    
    if plant_id:
        query += " AND plant_id = %s"
        params.append(plant_id)
    
    query += " ORDER BY date DESC"
    
    with get_db_connection() as conn:
        return pd.read_sql_query(query, conn, params=params)


def get_system_status_daily_data(
    start_date: datetime,
    end_date: datetime,
    plant_id: Optional[str] = None
) -> pd.DataFrame:
    """
    Fetch daily system status data for the specified date range.
    
    Args:
        start_date: Start date
        end_date: End date
        plant_id: Optional plant UUID filter
        
    Returns:
        DataFrame with daily system status data
    """
    query = """
        SELECT 
            plant_id,
            date,
            total_failures_sum,
            max_concurrent_failures,
            avg_failures,
            stress_level_avg,
            stress_level_max,
            stress_level_min,
            avg_healthy_components,
            min_healthy_components,
            avg_failed_components,
            max_failed_components,
            total_critical_failures,
            max_concurrent_critical,
            total_records,
            valid_records,
            processed_timestamp
        FROM gold.system_status_daily
        WHERE date >= %s AND date <= %s
    """
    
    params = [start_date.date(), end_date.date()]
    
    if plant_id:
        query += " AND plant_id = %s"
        params.append(plant_id)
    
    query += " ORDER BY date DESC"
    
    with get_db_connection() as conn:
        return pd.read_sql_query(query, conn, params=params)


def get_plant_daily_summary(
    start_date: datetime,
    end_date: datetime,
    plant_id: Optional[str] = None
) -> pd.DataFrame:
    """
    Fetch daily plant summary data for the specified date range.
    This is the main aggregated view for daily analysis.
    
    Args:
        start_date: Start date
        end_date: End date
        plant_id: Optional plant UUID filter
        
    Returns:
        DataFrame with daily plant summary data
    """
    query = """
        SELECT 
            plant_id,
            date,
            total_generation_kwh,
            total_consumption_kwh,
            net_energy_kwh,
            self_consumption_kwh,
            self_consumption_ratio,
            grid_export_kwh,
            grid_import_kwh,
            total_irradiation_kwh_m2,
            sunshine_hours,
            specific_yield_kwh_kwp,
            performance_ratio,
            capacity_factor,
            avg_ambient_temp_c,
            avg_module_temp_c,
            temp_loss_factor,
            plant_availability_percent,
            inverter_availability_avg,
            total_failures,
            critical_failures,
            inverter_failures,
            system_stress_avg,
            active_inverters,
            total_inverters,
            data_completeness_percent,
            processed_timestamp
        FROM gold.plant_daily_summary
        WHERE date >= %s AND date <= %s
    """
    
    params = [start_date.date(), end_date.date()]
    
    if plant_id:
        query += " AND plant_id = %s"
        params.append(plant_id)
    
    query += " ORDER BY date DESC"
    
    with get_db_connection() as conn:
        return pd.read_sql_query(query, conn, params=params)


def get_failure_daily_summary(
    start_date: datetime,
    end_date: datetime,
    plant_id: Optional[str] = None,
    component_type: Optional[str] = None
) -> pd.DataFrame:
    """
    Fetch daily failure summary data for the specified date range.
    
    Args:
        start_date: Start date
        end_date: End date
        plant_id: Optional plant UUID filter
        component_type: Optional component type filter ('inverter', 'meteo', 'poi_meter', 'system')
        
    Returns:
        DataFrame with daily failure summary data
    """
    query = """
        SELECT 
            plant_id,
            date,
            component_type,
            component_id,
            failure_count,
            critical_failure_count,
            failure_types,
            total_downtime_minutes,
            estimated_energy_loss_kwh,
            mean_time_between_failures_hours,
            mean_time_to_repair_minutes,
            processed_timestamp
        FROM gold.failure_daily_summary
        WHERE date >= %s AND date <= %s
    """
    
    params = [start_date.date(), end_date.date()]
    
    if plant_id:
        query += " AND plant_id = %s"
        params.append(plant_id)
    
    if component_type:
        query += " AND component_type = %s"
        params.append(component_type)
    
    query += " ORDER BY date DESC, component_type, component_id"
    
    with get_db_connection() as conn:
        return pd.read_sql_query(query, conn, params=params)


# ============================================================================
# UTILITY FUNCTIONS FOR DASHBOARD
# ============================================================================

def get_latest_data_timestamp(table_name: str, plant_id: Optional[str] = None) -> Optional[datetime]:
    """
    Get the latest timestamp from a gold table.
    
    Args:
        table_name: Name of the table (without 'gold.' prefix)
        plant_id: Optional plant UUID filter
        
    Returns:
        Latest timestamp or None if no data
    """
    # Determine the timestamp column based on table type
    if 'hourly' in table_name:
        timestamp_col = 'timestamp_hour'
    elif 'daily' in table_name:
        timestamp_col = 'processed_timestamp'
    else:
        timestamp_col = 'processed_timestamp'
    
    query = f"""
        SELECT MAX({timestamp_col}) as latest_timestamp
        FROM gold.{table_name}
    """
    
    params = []
    if plant_id:
        query += " WHERE plant_id = %s"
        params.append(plant_id)
    
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, params)
            result = cursor.fetchone()
            return result['latest_timestamp'] if result else None


def get_available_plants() -> List[Dict[str, Any]]:
    """
    Get list of all available plant IDs from the database.
    
    Returns:
        List of dictionaries with plant information
    """
    query = """
        SELECT DISTINCT plant_id
        FROM gold.plant_hourly_summary
        ORDER BY plant_id
    """
    
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            return [dict(row) for row in cursor.fetchall()]


def get_dashboard_realtime_data(
    lookback_hours: int = 24,
    plant_id: Optional[str] = None
) -> Dict[str, pd.DataFrame]:
    """
    Get all relevant data for a real-time dashboard.
    Fetches the last N hours of data from all hourly tables based on the latest
    timestamp in the database (not current system time). This works correctly
    with simulations that may have data in the "future" or past.
    
    Args:
        lookback_hours: Number of hours to look back from the latest data (default 24)
        plant_id: Optional plant UUID filter
        
    Returns:
        Dictionary with DataFrames for all data types
    """
    from datetime import timedelta
    
    # Get the latest timestamp from the database instead of using current time
    latest_timestamp = get_latest_data_timestamp('plant_hourly_summary', plant_id)
    
    if latest_timestamp is None:
        # No data available, return empty DataFrames
        return {
            'plant_summary': pd.DataFrame(),
            'inverters': pd.DataFrame(),
            'meteo': pd.DataFrame(),
            'poi_meter': pd.DataFrame(),
            'system_status': pd.DataFrame()
        }
    
    end_time = latest_timestamp
    start_time = end_time - timedelta(hours=lookback_hours)
    
    return {
        'plant_summary': get_plant_hourly_summary(start_time, end_time, plant_id),
        'inverters': get_inverter_hourly_data(start_time, end_time, plant_id),
        'meteo': get_meteo_hourly_data(start_time, end_time, plant_id),
        'poi_meter': get_poi_meter_hourly_data(start_time, end_time, plant_id),
        'system_status': get_system_status_hourly_data(start_time, end_time, plant_id)
    }


# ============================================================================
# INFERENCE DATA FUNCTIONS
# ============================================================================

def get_anomaly_predictions(
    start_datetime: datetime,
    end_datetime: datetime,
    plant_id: Optional[str] = None,
    inverter_id: Optional[str] = None
) -> pd.DataFrame:
    """
    Fetch anomaly detection predictions for the specified datetime range.
    
    Args:
        start_datetime: Start of the time range
        end_datetime: End of the time range
        plant_id: Optional plant UUID filter
        inverter_id: Optional inverter ID filter
        
    Returns:
        DataFrame with anomaly predictions
    """
    query = """
        SELECT 
            plant_id,
            inverter_id,
            timestamp,
            model_run_id,
            reconstruction_error,
            threshold,
            is_anomaly,
            created_at
        FROM gold.anomaly_predictions
        WHERE timestamp >= %s AND timestamp <= %s
    """
    
    params = [start_datetime, end_datetime]
    
    if plant_id:
        # Cast to UUID to ensure proper comparison
        query += " AND plant_id = %s::uuid"
        params.append(plant_id)
    
    if inverter_id:
        query += " AND inverter_id = %s"
        params.append(inverter_id)
    
    query += " ORDER BY timestamp DESC, inverter_id"
    
    with get_db_connection() as conn:
        return pd.read_sql_query(query, conn, params=params)


def get_forecast_predictions(
    start_datetime: datetime,
    end_datetime: datetime,
    plant_id: Optional[str] = None,
    inverter_id: Optional[str] = None
) -> pd.DataFrame:
    """
    Fetch forecasting predictions for the specified datetime range.
    
    Args:
        start_datetime: Start of the time range
        end_datetime: End of the time range
        plant_id: Optional plant UUID filter
        inverter_id: Optional inverter ID filter
        
    Returns:
        DataFrame with forecast predictions
    """
    query = """
        SELECT 
            plant_id,
            inverter_id,
            timestamp,
            model_run_id,
            predicted_ac_power_kw,
            created_at
        FROM gold.forecast_predictions
        WHERE timestamp >= %s AND timestamp <= %s
    """
    
    params = [start_datetime, end_datetime]
    
    if plant_id:
        # Cast to UUID to ensure proper comparison
        query += " AND plant_id = %s::uuid"
        params.append(plant_id)
    
    if inverter_id:
        query += " AND inverter_id = %s"
        params.append(inverter_id)
    
    query += " ORDER BY timestamp DESC, inverter_id"
    
    with get_db_connection() as conn:
        return pd.read_sql_query(query, conn, params=params)


def get_latest_anomalies(
    plant_id: Optional[str] = None,
    limit: int = 20
) -> pd.DataFrame:
    """
    Fetch the most recent anomaly detections.
    
    Args:
        plant_id: Optional plant UUID filter
        limit: Maximum number of anomalies to return
        
    Returns:
        DataFrame with recent anomalies
    """
    query = """
        SELECT 
            plant_id,
            inverter_id,
            timestamp,
            reconstruction_error,
            threshold,
            is_anomaly
        FROM gold.anomaly_predictions
        WHERE is_anomaly = 1
    """
    
    params = []
    
    if plant_id:
        # Cast to UUID to ensure proper comparison
        query += " AND plant_id = %s::uuid"
        params.append(plant_id)
    
    query += f" ORDER BY timestamp DESC LIMIT {limit}"
    
    with get_db_connection() as conn:
        return pd.read_sql_query(query, conn, params=params)


def get_dashboard_data_with_inference(
    lookback_hours: int = 24,
    forecast_horizon_hours: int = 24,
    plant_id: Optional[str] = None
) -> Dict[str, pd.DataFrame]:
    """
    Get all relevant data for the dashboard including inference results.
    
    Args:
        lookback_hours: Number of hours to look back from the latest data
        forecast_horizon_hours: Number of hours ahead to fetch future forecasts
        plant_id: Optional plant UUID filter
        
    Returns:
        Dictionary with DataFrames for all data types including inference
    """
    from datetime import timedelta
    
    latest_timestamp = get_latest_data_timestamp('plant_hourly_summary', plant_id)
    
    if latest_timestamp is None:
        return {
            'plant_summary': pd.DataFrame(),
            'inverters': pd.DataFrame(),
            'meteo': pd.DataFrame(),
            'anomaly_predictions': pd.DataFrame(),
            'forecast_predictions': pd.DataFrame(),
            'latest_anomalies': pd.DataFrame()
        }
    
    end_time = pd.Timestamp(latest_timestamp).to_pydatetime()
    start_time = end_time - timedelta(hours=lookback_hours)
    forecast_end_time = end_time + timedelta(hours=forecast_horizon_hours)
    
    return {
        'plant_summary': get_plant_hourly_summary(start_time, end_time, plant_id),
        'inverters': get_inverter_hourly_data(start_time, end_time, plant_id),
        'meteo': get_meteo_hourly_data(start_time, end_time, plant_id),
        'anomaly_predictions': get_anomaly_predictions(start_time, end_time, plant_id),
        # Include forecasts from lookback window through future horizon
        # This ensures we show forecasts even if they were generated before latest data
        'forecast_predictions': get_forecast_predictions(start_time, forecast_end_time, plant_id),
        'latest_anomalies': get_latest_anomalies(plant_id, limit=20)
    }