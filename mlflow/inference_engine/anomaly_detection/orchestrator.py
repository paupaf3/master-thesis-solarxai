import torch
import numpy as np
from .model_loader import load_model_and_preprocessing
from .db_reader import fetch_new_rows
from .db_writer import write_predictions
from .preprocessor import preprocess_features


def reconstruction_errors(model, X_np, device):
    model.eval()
    with torch.no_grad():
        x_t = torch.tensor(X_np, dtype=torch.float32).to(device)
        x_hat = model(x_t)
        errors = torch.mean((x_hat - x_t) ** 2, dim=1).cpu().numpy()
    return errors


def predict_anomalies(model, X_scaled, preprocessing, device):
    errors = reconstruction_errors(model, X_scaled.values, device)
    threshold = preprocessing["threshold"]
    is_anomaly = (errors > threshold).astype(int)
    return {
        "reconstruction_errors": errors,
        "threshold": threshold,
        "is_anomaly": is_anomaly,
    }


def run(cfg):
    device = torch.device(cfg.device)
    inverter_ids = cfg.inverter_ids or []
    for inverter_id in inverter_ids:
        try:
            run_info, model, preprocessing = load_model_and_preprocessing(cfg, inverter_id)
            new_rows = fetch_new_rows(cfg, inverter_id)
            if new_rows.empty:
                print(f"[anomaly] No new rows for inverter {inverter_id}, skipping.")
                continue
            X_scaled = preprocess_features(new_rows, preprocessing)
            preds = predict_anomalies(model, X_scaled, preprocessing, device)
            write_predictions(cfg, inverter_id, new_rows, preds, run_info)
            print(f"[anomaly] Wrote {len(new_rows)} predictions for inverter {inverter_id}.")
        except Exception as exc:
            print(f"[anomaly] Error processing inverter {inverter_id}: {exc}")