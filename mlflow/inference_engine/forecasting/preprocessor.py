import pandas as pd
import numpy as np


def compute_temporal_features(df: pd.DataFrame):
    """
    Compute temporal features from timestamps.
    
    These features are known in advance for any future timestamp,
    enabling true forecasting (not just fitting historical data).
    
    Matches the features used in training (after LEAKAGE_COLS removal):
    - hour, day_of_week, day_of_month, month, quarter, is_weekend
    - hour_sin, hour_cos, day_of_week_sin, day_of_week_cos, month_sin, month_cos
    """
    df = df.copy()
    df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True, errors='coerce')

    # Time-based features
    df['hour'] = df['timestamp'].dt.hour
    df['day_of_week'] = df['timestamp'].dt.dayofweek
    df['day_of_month'] = df['timestamp'].dt.day
    df['month'] = df['timestamp'].dt.month
    df['quarter'] = df['timestamp'].dt.quarter
    df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)

    # Cyclic encoding
    df['hour_sin'] = np.sin(2 * np.pi * df['hour'] / 24)
    df['hour_cos'] = np.cos(2 * np.pi * df['hour'] / 24)
    df['day_of_week_sin'] = np.sin(2 * np.pi * df['day_of_week'] / 7)
    df['day_of_week_cos'] = np.cos(2 * np.pi * df['day_of_week'] / 7)
    df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
    df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)

    return df


def preprocess_features(df: pd.DataFrame, preprocessing: dict):
    """
    Generate features for forecasting from timestamps only.
    
    Since the model was trained without data leakage columns (sensor readings,
    electrical measurements, etc.), we only need temporal features which are
    known for any future timestamp.
    """
    df = compute_temporal_features(df)
    feature_columns = preprocessing["feature_snapshot"]["feature_columns"]
    
    # Verify all required features can be computed from timestamps
    temporal_features = {
        'hour', 'day_of_week', 'day_of_month', 'month', 'quarter', 'is_weekend',
        'hour_sin', 'hour_cos', 'day_of_week_sin', 'day_of_week_cos',
        'month_sin', 'month_cos'
    }
    
    missing_features = set(feature_columns) - temporal_features - set(df.columns)
    if missing_features:
        raise ValueError(
            f"Model requires features that cannot be computed from timestamps alone: {missing_features}. "
            f"This model may have been trained with data leakage. "
            f"Expected only temporal features: {temporal_features}"
        )
    
    X = df[feature_columns].copy()
    X = X.fillna(0)
    return X