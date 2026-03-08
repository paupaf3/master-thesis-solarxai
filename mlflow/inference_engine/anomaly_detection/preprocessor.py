import pandas as pd
import numpy as np


def preprocess_features(df: pd.DataFrame, preprocessing: dict):
    """Apply feature selection and standardization matching training pipeline."""
    feature_columns = preprocessing["feature_snapshot"]["feature_columns"]
    X = df[feature_columns].copy()

    # Standardize using saved scaler mean and std
    mean = preprocessing["scaler_mean"]
    std = preprocessing["scaler_std"]
    X_scaled = (X - mean) / std
    X_scaled = X_scaled.replace([np.inf, -np.inf], np.nan).fillna(0).astype(np.float32)

    return X_scaled