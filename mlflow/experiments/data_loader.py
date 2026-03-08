import pandas as pd
import numpy as np
import csv_loader as csvl

BRONZE_DATA_PATH = "data/bronze/"

INVERTER_RAW_DATA_PATH = BRONZE_DATA_PATH + "inverter_raw.csv"
METEOSTATION_RAW_DATA_PATH = BRONZE_DATA_PATH + "meteo_station_raw.csv"
POIMETER_RAW_DATA_PATH = BRONZE_DATA_PATH + "poi_meter_raw.csv"


def stat_data_loader():
    # Load data
    inverter_raw = csvl.load_csv(INVERTER_RAW_DATA_PATH)
    
    # Select only the relevant columns for the inverter data
    inverter = inverter_raw[['inverter_id', 'timestamp', 'ac_power_kw', 'active_failures']].copy()
    
    
    inverter["timestamp"] = pd.to_datetime(inverter["timestamp"], format='ISO8601')
    inverter["ac_power_kw"] = inverter["ac_power_kw"].astype(float)
    inverter["active_failures"] = inverter["active_failures"].astype(int)

    inverter = inverter.sort_values("timestamp").reset_index(drop=True)
    
    # Remove malfunctioning timesteps
    inverter = inverter[inverter['active_failures'] == 0].copy()
    inverter = inverter.copy()
    inverter.drop(columns=['active_failures'], inplace=True)
    
    # Resample data to get a constant frequency of 1 minute
    # It must be done for each inverter separately
    
    resampled_datasets = []

    for inverter_id in inverter['inverter_id'].unique():
        inverter_data = inverter[inverter['inverter_id'] == inverter_id].copy()
        
        # Set timestamp as index
        inverter_data = inverter_data.set_index('timestamp')
        
        # Resample only the ac_power_kw column to 1 minute frequency
        ac_power_resampled = inverter_data[['ac_power_kw']].resample('1min').mean()
        
        # Interpolate missing values (linear interpolation for AC power)
        ac_power_resampled['ac_power_kw'] = ac_power_resampled['ac_power_kw'].interpolate(method='linear', limit_direction='both')
        
        # Add inverter_id back as a column
        ac_power_resampled['inverter_id'] = inverter_id
        
        # Reset index to get timestamp back as a column
        ac_power_resampled = ac_power_resampled.reset_index()
        
        resampled_datasets.append(ac_power_resampled)
        
        print(f"Inverter {inverter_id}: resampled {len(ac_power_resampled)} records")

    # Combine all inverters
    inverter = pd.concat(resampled_datasets, ignore_index=True)

    # Sort by inverter_id and timestamp
    inverter = inverter.sort_values(['inverter_id', 'timestamp']).reset_index(drop=True)    
    
    return inverter


def ml_data_loader():
    inverter_raw = csvl.load_csv(INVERTER_RAW_DATA_PATH)
    meteo_station_raw = csvl.load_csv(METEOSTATION_RAW_DATA_PATH)
    
    # Start with inverter data
    ml_dataset = inverter_raw.copy()
    
    ml_dataset['timestamp'] = pd.to_datetime(ml_dataset['timestamp'], utc=True, errors='coerce')
    meteo_station_raw['timestamp'] = pd.to_datetime(meteo_station_raw['timestamp'], utc=True, errors='coerce')
    
    # Drop bad rows and sort (required by merge_asof)
    ml_dataset = ml_dataset.dropna(subset=['timestamp']).sort_values('timestamp').reset_index(drop=True)
    meteo_station_raw = meteo_station_raw.dropna(subset=['timestamp']).sort_values('timestamp').reset_index(drop=True)

    # Merge with meteo station data
    ml_dataset = pd.merge_asof(ml_dataset, meteo_station_raw, on='timestamp', direction='nearest', tolerance=pd.Timedelta('1min'))

    numeric_cols = [
        'ac_power_kw',
        'dc_power_kw',
        'healthy_strings',
        'module_temp_c',
        'amb_temp_c',
        'poa_irradiance_wm2',
        'wind_speed_ms',
        'wind_dir_deg',
        'humidity_percent',
        'dc_voltage_v',
        'dc_current_a',
        'ac_freq_hz',
        'active_failures',
        'failed_strings',
    ]

    for col in numeric_cols:
        if col in ml_dataset.columns:
            ml_dataset[col] = (
                ml_dataset[col]
                .astype(str)
                .str.replace(',', '.', regex=False)
                .str.strip()
            )
            ml_dataset[col] = pd.to_numeric(ml_dataset[col], errors='coerce')

    required_for_features = [
        'ac_power_kw',
        'dc_power_kw',
        'healthy_strings',
        'module_temp_c',
        'amb_temp_c',
        'poa_irradiance_wm2',
    ]
    existing_required = [c for c in required_for_features if c in ml_dataset.columns]
    ml_dataset = ml_dataset.dropna(subset=existing_required)

    # Time-based features
    ml_dataset['hour'] = ml_dataset['timestamp'].dt.hour
    ml_dataset['day_of_week'] = ml_dataset['timestamp'].dt.dayofweek
    ml_dataset['day_of_month'] = ml_dataset['timestamp'].dt.day
    ml_dataset['month'] = ml_dataset['timestamp'].dt.month
    ml_dataset['quarter'] = ml_dataset['timestamp'].dt.quarter
    ml_dataset['is_weekend'] = ml_dataset['day_of_week'].isin([5, 6]).astype(int)

    # Cyclic encoding for time features (to preserve cyclical nature)
    ml_dataset['hour_sin'] = np.sin(2 * np.pi * ml_dataset['hour'] / 24)
    ml_dataset['hour_cos'] = np.cos(2 * np.pi * ml_dataset['hour'] / 24)
    ml_dataset['day_of_week_sin'] = np.sin(2 * np.pi * ml_dataset['day_of_week'] / 7)
    ml_dataset['day_of_week_cos'] = np.cos(2 * np.pi * ml_dataset['day_of_week'] / 7)
    ml_dataset['month_sin'] = np.sin(2 * np.pi * ml_dataset['month'] / 12)
    ml_dataset['month_cos'] = np.cos(2 * np.pi * ml_dataset['month'] / 12)
    
    # For now no lag features or rolling statistics
    ml_dataset = ml_dataset.sort_values(['inverter_id', 'timestamp']).reset_index(drop=True)
    
    # Additional features
    # Ratio of DC to AC power (efficiency indicator)
    ml_dataset['dc_to_ac_ratio'] = ml_dataset['dc_power_kw'] / (ml_dataset['ac_power_kw'] + 1e-6)

    # Power per healthy string
    ml_dataset['power_per_healthy_string'] = ml_dataset['ac_power_kw'] / (ml_dataset['healthy_strings'] + 1)

    # Temperature difference
    ml_dataset['temp_diff'] = ml_dataset['module_temp_c'] - ml_dataset['amb_temp_c']

    # Interaction features
    ml_dataset['irradiance_temp_interaction'] = ml_dataset['poa_irradiance_wm2'] * ml_dataset['amb_temp_c']

    processed_columns = [
        'timestamp',
        'inverter_id',
        'state',
        'ac_power_kw',
        'inverter_temp_c',
        'ac_freq_hz',
        'dc_power_kw',
        'dc_voltage_v',
        'dc_current_a',
        'active_failures',
        'healthy_strings',
        'failed_strings',
        'amb_temp_c',
        'module_temp_c',
        'wind_speed_ms',
        'wind_dir_deg',
        'humidity_percent',
        'poa_irradiance_wm2',
        'hour',
        'day_of_week',
        'day_of_month',
        'month',
        'quarter',
        'is_weekend',
        'hour_sin',
        'hour_cos',
        'day_of_week_sin',
        'day_of_week_cos',
        'month_sin',
        'month_cos',
        'dc_to_ac_ratio',
        'power_per_healthy_string',
        'temp_diff',
        'irradiance_temp_interaction',
    ]

    existing_processed_cols = [c for c in processed_columns if c in ml_dataset.columns]
    ml_dataset = ml_dataset[existing_processed_cols].copy()
    
    # inverter_df = _inverter_data_preprocessing(inverter_df)
    # meteo_station_df = _meteo_station_data_preprocessing(meteo_station_df)
    # poi_meter_df = _poi_meter_data_preprocessing(poi_meter_df)
    
    return ml_dataset


def dl_ad_data_loader():
    inverter_raw = csvl.load_csv(INVERTER_RAW_DATA_PATH)
    meteo_station_raw = csvl.load_csv(METEOSTATION_RAW_DATA_PATH)

    inverter_cols = [
        'inverter_id',
        'timestamp',
        'inverter_temp_c',
        'ac_power_kw',
        'ac_freq_hz',
        'dc_power_kw',
        'dc_voltage_v',
        'dc_current_a',
        'healthy_strings',
        'failed_strings',
        'active_failures',
    ]
    meteo_cols = [
        'timestamp',
        'amb_temp_c',
        'module_temp_c',
        'wind_speed_ms',
        'wind_dir_deg',
        'humidity_percent',
        'poa_irradiance_wm2',
    ]

    ad_inverter = inverter_raw[inverter_cols].copy()
    ad_meteo = meteo_station_raw[meteo_cols].copy()

    ad_inverter['timestamp'] = pd.to_datetime(ad_inverter['timestamp'], utc=True, errors='coerce')
    ad_meteo['timestamp'] = pd.to_datetime(ad_meteo['timestamp'], utc=True, errors='coerce')

    numeric_cols = [
        'inverter_temp_c',
        'ac_power_kw',
        'ac_freq_hz',
        'dc_power_kw',
        'dc_voltage_v',
        'dc_current_a',
        'healthy_strings',
        'failed_strings',
        'active_failures',
        'amb_temp_c',
        'module_temp_c',
        'wind_speed_ms',
        'wind_dir_deg',
        'humidity_percent',
        'poa_irradiance_wm2',
    ]

    for col in numeric_cols:
        if col in ad_inverter.columns:
            ad_inverter[col] = (
                ad_inverter[col]
                .astype(str)
                .str.replace(',', '.', regex=False)
                .str.strip()
            )
            ad_inverter[col] = pd.to_numeric(ad_inverter[col], errors='coerce')

        if col in ad_meteo.columns:
            ad_meteo[col] = (
                ad_meteo[col]
                .astype(str)
                .str.replace(',', '.', regex=False)
                .str.strip()
            )
            ad_meteo[col] = pd.to_numeric(ad_meteo[col], errors='coerce')

    ad_inverter = ad_inverter.dropna(subset=['timestamp']).sort_values('timestamp').reset_index(drop=True)
    ad_meteo = ad_meteo.dropna(subset=['timestamp']).sort_values('timestamp').reset_index(drop=True)

    dl_ad_dataset = pd.merge_asof(
        ad_inverter,
        ad_meteo,
        on='timestamp',
        direction='nearest',
        tolerance=pd.Timedelta('1min'),
    )

    if 'active_failures' not in dl_ad_dataset.columns:
        raise ValueError("Column 'active_failures' is required to build 'is_anomaly'")

    dl_ad_dataset['active_failures'] = pd.to_numeric(dl_ad_dataset['active_failures'], errors='coerce').fillna(0)
    dl_ad_dataset['is_anomaly'] = (dl_ad_dataset['active_failures'] > 0).astype(int)

    dl_ad_dataset = dl_ad_dataset.ffill().bfill().dropna().copy()
    dl_ad_dataset = dl_ad_dataset.sort_values(['inverter_id', 'timestamp']).reset_index(drop=True)

    return dl_ad_dataset