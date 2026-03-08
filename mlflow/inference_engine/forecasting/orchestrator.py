import numpy as np
from .model_loader import load_model_and_preprocessing
from .db_reader import generate_future_timestamps
from .db_writer import write_predictions
from .preprocessor import preprocess_features


def run(cfg):
    """
    Run forecasting inference for all configured inverters.
    
    For each inverter:
    1. Load the best trained model and preprocessing config
    2. Generate future timestamps (next `forecast_horizon_hours` hours)
    3. Compute temporal features for those timestamps
    4. Predict ac_power_kw using the model
    5. Write predictions to database
    """
    inverter_ids = cfg.inverter_ids or []
    horizon_hours = cfg.forecast_horizon_hours
    freq_minutes = cfg.forecast_frequency_minutes
    
    for inverter_id in inverter_ids:
        try:
            run_info, model, preprocessing = load_model_and_preprocessing(cfg, inverter_id)
            
            future_df = generate_future_timestamps(cfg, inverter_id)
            if future_df.empty:
                print(f"[forecasting] No future timestamps to predict for inverter {inverter_id}, skipping.")
                continue
            
            X = preprocess_features(future_df, preprocessing)
            preds = np.clip(model.predict(X), 0, None)
            
            write_predictions(cfg, inverter_id, future_df, preds, run_info)
            
            ts_min = future_df['timestamp'].min()
            ts_max = future_df['timestamp'].max()
            print(
                f"[forecasting] Wrote {len(future_df)} predictions for inverter {inverter_id} "
                f"({horizon_hours}h ahead, {freq_minutes}min intervals): {ts_min} to {ts_max}"
            )
        except Exception as exc:
            print(f"[forecasting] Error processing inverter {inverter_id}: {exc}")