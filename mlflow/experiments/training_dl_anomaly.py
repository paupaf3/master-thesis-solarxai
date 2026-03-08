import mlflow
import mlflow.pytorch
import optuna
import data_loader as dl
import numpy as np
import pandas as pd
import hashlib
import matplotlib.pyplot as plt
import subprocess
import tempfile
import torch
from pathlib import Path
import matplotlib
matplotlib.use('Agg')

from sklearn.model_selection import TimeSeriesSplit
from mlflow.models import infer_signature

SEED = 42
INVERTER_IDS = None
TARGET_COL = 'is_anomaly'
ID_COLS = ['inverter_id', 'state']
TRAIN_PERC = 0.7
N_SPLITS = 3
N_TRIALS = 5

LEAKAGE_COLS = []

def initialize_mlflow_config():
    mlflow.set_experiment("Inverter Anomaly Detection using DL Autoencoder")
    # This enables system metrics - commonly used for deep learning
    mlflow.config.enable_system_metrics_logging()
    # Log every 1 seconds
    mlflow.config.set_system_metrics_sampling_interval(1)
    
    
def get_git_commit() -> str:
    try:
        return subprocess.check_output(
            ['git', 'rev-parse', 'HEAD'],
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
    except Exception:
        return 'unknown'


def to_iso_timestamp(value) -> str:
    return pd.Timestamp(value).isoformat()


def compute_data_fingerprint(data: pd.DataFrame) -> str:
    sortable = data.copy()
    sortable = sortable.sort_values(['inverter_id', 'timestamp']).reset_index(drop=True)
    sortable = sortable.reindex(sorted(sortable.columns), axis=1)
    hashed = pd.util.hash_pandas_object(sortable, index=False).values.tobytes()
    return hashlib.sha256(hashed).hexdigest()


def load_data():
    ad_dataset = dl.dl_ad_data_loader()

    required_cols = {'timestamp', 'inverter_id', TARGET_COL}
    missing_required = required_cols - set(ad_dataset.columns)
    if missing_required:
        missing_as_text = ", ".join(sorted(missing_required))
        raise ValueError(f"Dataset is missing required columns: {missing_as_text}")

    ad_dataset = ad_dataset.dropna(subset=['timestamp']).copy()
    ad_dataset['timestamp'] = pd.to_datetime(ad_dataset['timestamp'], utc=True, errors='coerce')
    ad_dataset[TARGET_COL] = pd.to_numeric(ad_dataset[TARGET_COL], errors='coerce').fillna(0).astype(int)
    ad_dataset = ad_dataset.dropna(subset=['timestamp'])

    if INVERTER_IDS:
        ad_dataset = ad_dataset[ad_dataset['inverter_id'].isin(INVERTER_IDS)].copy()

    ad_dataset = ad_dataset.sort_values(['inverter_id', 'timestamp']).reset_index(drop=True)

    train_datasets = []
    test_datasets = []

    for inverter_id in sorted(ad_dataset['inverter_id'].dropna().astype(str).unique().tolist()):
        inv_data = ad_dataset[ad_dataset['inverter_id'].astype(str) == str(inverter_id)].copy()
        inv_data = inv_data.sort_values('timestamp').reset_index(drop=True)

        normal_data = inv_data[inv_data[TARGET_COL] == 0].copy()
        anomalous_data = inv_data[inv_data[TARGET_COL] == 1].copy()

        if normal_data.empty:
            continue

        train_size = int(len(normal_data) * TRAIN_PERC)
        train_data = normal_data.iloc[:train_size].copy()
        val_data = normal_data.iloc[train_size:].copy()

        test_data = pd.concat([val_data, anomalous_data], ignore_index=True)

        if not train_data.empty:
            train_datasets.append(train_data)
        if not test_data.empty:
            test_datasets.append(test_data)

    if not train_datasets or not test_datasets:
        raise ValueError('Unable to build train/test datasets from dl_ad_data_loader output')

    train_df = pd.concat(train_datasets, ignore_index=True).sort_values(['inverter_id', 'timestamp']).reset_index(drop=True)
    test_df = pd.concat(test_datasets, ignore_index=True).sort_values(['inverter_id', 'timestamp']).reset_index(drop=True)

    data = {'train': train_df, 'test': test_df}
    full_for_fingerprint = pd.concat([train_df.assign(split='train'), test_df.assign(split='test')], axis=0)
    data_meta = {
        'train_rows': int(len(train_df)),
        'test_rows': int(len(test_df)),
        'dataset_cols': int(len(train_df.columns)),
        'dataset_min_timestamp': to_iso_timestamp(min(train_df['timestamp'].min(), test_df['timestamp'].min())),
        'dataset_max_timestamp': to_iso_timestamp(max(train_df['timestamp'].max(), test_df['timestamp'].max())),
        'dataset_fingerprint_sha256': compute_data_fingerprint(full_for_fingerprint),
    }
    return data, data_meta


def prepare_inverter_data(data: dict, inverter_id: str):
    train_raw = data['train'][data['train']['inverter_id'].astype(str) == str(inverter_id)].copy()
    test_raw = data['test'][data['test']['inverter_id'].astype(str) == str(inverter_id)].copy()

    if train_raw.empty or test_raw.empty:
        raise ValueError(f"Missing train/test rows for inverter {inverter_id}")

    train_raw = train_raw.set_index('timestamp').sort_index()
    test_raw = test_raw.set_index('timestamp').sort_index()

    drop_cols = [c for c in (ID_COLS + LEAKAGE_COLS) if c in train_raw.columns]

    if TARGET_COL not in train_raw.columns or TARGET_COL not in test_raw.columns:
        raise ValueError(f"Target column '{TARGET_COL}' not found for inverter {inverter_id}")

    train_model = train_raw.drop(columns=drop_cols)
    test_model = test_raw.drop(columns=drop_cols)

    train_model = train_model.select_dtypes(include=[np.number]).copy()
    test_model = test_model.select_dtypes(include=[np.number]).copy()

    if TARGET_COL not in train_model.columns or TARGET_COL not in test_model.columns:
        raise ValueError(f"Target column '{TARGET_COL}' removed during preprocessing for inverter {inverter_id}")

    train_before = len(train_model)
    test_before = len(test_model)
    train_model = train_model.replace([np.inf, -np.inf], np.nan).dropna().copy()
    test_model = test_model.replace([np.inf, -np.inf], np.nan).dropna().copy()
    na_rows_removed = int((train_before - len(train_model)) + (test_before - len(test_model)))

    y_train = train_model[TARGET_COL].astype(int)
    y_test = test_model[TARGET_COL].astype(int)

    X_train = train_model.drop(columns=[TARGET_COL])
    X_test = test_model.drop(columns=[TARGET_COL])

    if len(X_train) < 100 or len(X_test) < 50:
        raise ValueError(
            f"Not enough rows for inverter {inverter_id}: train={len(X_train)}, test={len(X_test)}"
        )

    feature_columns = X_train.columns.tolist()

    preprocessing_info = {
        'dropped_columns': drop_cols,
        'feature_columns': feature_columns,
        'na_rows_removed': na_rows_removed,
        'train_start': to_iso_timestamp(X_train.index.min()),
        'train_end': to_iso_timestamp(X_train.index.max()),
        'test_start': to_iso_timestamp(X_test.index.min()),
        'test_end': to_iso_timestamp(X_test.index.max()),
    }

    return X_train, X_test, y_train, y_test, preprocessing_info


def calculate_metrics(y_true, y_pred):
    y_true = np.asarray(y_true).astype(int)
    y_pred = np.asarray(y_pred).astype(int)

    tp = int(((y_true == 1) & (y_pred == 1)).sum())
    tn = int(((y_true == 0) & (y_pred == 0)).sum())
    fp = int(((y_true == 0) & (y_pred == 1)).sum())
    fn = int(((y_true == 1) & (y_pred == 0)).sum())

    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0

    return {
        'tp': tp,
        'tn': tn,
        'fp': fp,
        'fn': fn,
        'precision': float(precision),
        'recall': float(recall),
        'f1': float(f1),
    }


class Autoencoder(torch.nn.Module):
    def __init__(self, input_dim: int, hidden_dim: int, bottleneck_dim: int):
        super().__init__()
        self.encoder = torch.nn.Sequential(
            torch.nn.Linear(input_dim, hidden_dim),
            torch.nn.ReLU(),
            torch.nn.Linear(hidden_dim, bottleneck_dim),
            torch.nn.ReLU(),
        )
        self.decoder = torch.nn.Sequential(
            torch.nn.Linear(bottleneck_dim, hidden_dim),
            torch.nn.ReLU(),
            torch.nn.Linear(hidden_dim, input_dim),
        )

    def forward(self, x):
        z = self.encoder(x)
        return self.decoder(z)


def standardize_train_test(X_train: pd.DataFrame, X_test: pd.DataFrame):
    mean = X_train.mean()
    std = X_train.std().replace(0, 1.0)
    X_train_scaled = ((X_train - mean) / std).astype(np.float32)
    X_test_scaled = ((X_test - mean) / std).astype(np.float32)
    return X_train_scaled, X_test_scaled, mean, std


def build_model_from_params(input_dim: int, params: dict):
    hidden_dim = max(8, int(input_dim * params['hidden_ratio']))
    bottleneck_dim = max(4, int(hidden_dim * params['bottleneck_ratio']))
    return Autoencoder(input_dim=input_dim, hidden_dim=hidden_dim, bottleneck_dim=bottleneck_dim)


def train_autoencoder(model, X_train_np: np.ndarray, epochs: int, batch_size: int, lr: float, device):
    dataset = torch.utils.data.TensorDataset(torch.tensor(X_train_np, dtype=torch.float32))
    loader = torch.utils.data.DataLoader(dataset, batch_size=batch_size, shuffle=True)

    optimizer = torch.optim.Adam(model.parameters(), lr=lr)
    criterion = torch.nn.MSELoss()

    model.to(device)
    model.train()

    for _ in range(epochs):
        for (batch_x,) in loader:
            batch_x = batch_x.to(device)
            optimizer.zero_grad()
            recon = model(batch_x)
            loss = criterion(recon, batch_x)
            loss.backward()
            optimizer.step()

    return model


def reconstruction_errors(model, X_np: np.ndarray, device):
    model.eval()
    with torch.no_grad():
        x_t = torch.tensor(X_np, dtype=torch.float32).to(device)
        x_hat = model(x_t)
        err = torch.mean((x_hat - x_t) ** 2, dim=1).cpu().numpy()
    return err


def evaluate_model(model, X_val, y_val):
    train_errors = reconstruction_errors(model, X_val['train'], X_val['device'])
    val_errors = reconstruction_errors(model, X_val['val'], X_val['device'])

    threshold = float(np.quantile(train_errors, X_val['threshold_quantile']))
    y_pred = (val_errors > threshold).astype(int)
    metrics = calculate_metrics(y_val, y_pred)
    metrics['threshold'] = threshold
    return metrics


def objective(trial, X_train, y_train):
    params = {
        'epochs': trial.suggest_int('epochs', 20, 40),
        'batch_size': trial.suggest_categorical('batch_size', [64, 128]),
        'lr': trial.suggest_float('lr', 1e-4, 5e-3, log=True),
        'hidden_ratio': trial.suggest_float('hidden_ratio', 0.3, 0.8),
        'bottleneck_ratio': trial.suggest_float('bottleneck_ratio', 0.3, 0.5),
        'threshold_quantile': trial.suggest_float('threshold_quantile', 0.990, 0.999),
    }

    tscv = TimeSeriesSplit(n_splits=N_SPLITS)
    fold_f1 = []
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

    for train_idx, val_idx in tscv.split(X_train):
        X_fold_train = X_train.iloc[train_idx]
        X_fold_val = X_train.iloc[val_idx]
        y_fold_val = y_train.iloc[val_idx]

        X_fold_train_s, X_fold_val_s, _, _ = standardize_train_test(X_fold_train, X_fold_val)
        model = build_model_from_params(X_fold_train_s.shape[1], params)
        model = train_autoencoder(
            model=model,
            X_train_np=X_fold_train_s.values,
            epochs=params['epochs'],
            batch_size=params['batch_size'],
            lr=params['lr'],
            device=device,
        )

        metrics = evaluate_model(
            model,
            {
                'train': X_fold_train_s.values,
                'val': X_fold_val_s.values,
                'threshold_quantile': params['threshold_quantile'],
                'device': device,
            },
            y_fold_val.values,
        )
        fold_f1.append(metrics['f1'])

    return 1.0 - float(np.mean(fold_f1))


def tune_hyperparameters(X_train, y_train):
    sampler = optuna.samplers.TPESampler(seed=SEED)
    pruner = optuna.pruners.MedianPruner(n_startup_trials=5)
    study = optuna.create_study(direction='minimize', sampler=sampler, pruner=pruner)
    study.optimize(lambda trial: objective(trial, X_train, y_train), n_trials=N_TRIALS)
    return study


def train_for_inverter(data: pd.DataFrame, inverter_id: str):
    X_train, X_test, y_train, y_test, preprocessing_info = prepare_inverter_data(data, inverter_id)

    study = tune_hyperparameters(X_train, y_train)
    best_params = dict(study.best_params)

    X_train_scaled, X_test_scaled, mean, std = standardize_train_test(X_train, X_test)

    model = build_model_from_params(X_train_scaled.shape[1], best_params)
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    model = train_autoencoder(
        model=model,
        X_train_np=X_train_scaled.values,
        epochs=best_params['epochs'],
        batch_size=best_params['batch_size'],
        lr=best_params['lr'],
        device=device,
    )

    train_errors = reconstruction_errors(model, X_train_scaled.values, device)
    test_errors = reconstruction_errors(model, X_test_scaled.values, device)
    threshold = float(np.quantile(train_errors, best_params['threshold_quantile']))
    y_pred_test = (test_errors > threshold).astype(int)
    y_pred_train = (train_errors > threshold).astype(int)

    train_metrics = calculate_metrics(y_train.values, y_pred_train)
    test_metrics = calculate_metrics(y_test.values, y_pred_test)
    test_metrics['threshold'] = threshold

    predictions_df = pd.DataFrame(
        {
            'timestamp': X_test.index,
            'y_true': y_test.values.astype(int),
            'y_pred': y_pred_test.astype(int),
            'reconstruction_error': test_errors,
            'threshold': threshold,
        }
    )
    predictions_df['is_correct'] = (predictions_df['y_true'] == predictions_df['y_pred']).astype(int)

    trials_df = study.trials_dataframe(attrs=('number', 'value', 'state', 'params'))
    trials_df = trials_df[trials_df['state'] == 'COMPLETE'].sort_values('value', ascending=True).head(5)

    input_example = X_train_scaled.head(min(5, len(X_train_scaled))).copy()

    normal_mask = y_test.values == 0
    anomaly_mask = y_test.values == 1
    err_normal = float(np.mean(test_errors[normal_mask])) if normal_mask.any() else 0.0
    err_anomaly = float(np.mean(test_errors[anomaly_mask])) if anomaly_mask.any() else 0.0

    scaler_stats = {
        'mean': mean,
        'std': std,
    }

    return {
        'inverter_id': inverter_id,
        'n_rows': int(len(X_train) + len(X_test)),
        'n_features': int(X_train.shape[1]),
        'n_train': int(len(X_train)),
        'n_test': int(len(X_test)),
        'best_params': best_params,
        'best_cv_f1': float(1.0 - study.best_value),
        'best_cv_objective': float(study.best_value),
        'train_metrics': train_metrics,
        'test_metrics': test_metrics,
        'preprocessing_info': preprocessing_info,
        'predictions_df': predictions_df,
        'top_trials_df': trials_df,
        'best_trial_number': int(study.best_trial.number),
        'model': model,
        'input_example': input_example,
        'threshold': threshold,
        'train_errors': train_errors,
        'test_errors': test_errors,
        'err_normal_mean': err_normal,
        'err_anomaly_mean': err_anomaly,
        'scaler_stats': scaler_stats,
    }


def log_dataframe_artifact(df: pd.DataFrame, filename: str, artifact_path: str):
    with tempfile.TemporaryDirectory() as tmp_dir:
        file_path = Path(tmp_dir) / filename
        df.to_csv(file_path, index=False)
        mlflow.log_artifact(str(file_path), artifact_path=artifact_path)


def log_diagnostic_plots(predictions_df: pd.DataFrame):
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)

        plt.figure(figsize=(8, 4))
        plt.hist(predictions_df['reconstruction_error'], bins=40)
        plt.title('Reconstruction Error Distribution (Test)')
        plt.xlabel('reconstruction_error')
        plt.ylabel('count')
        plt.tight_layout()
        error_hist_path = tmp_path / 'reconstruction_error_hist_test.png'
        plt.savefig(error_hist_path)
        plt.close()

        conf_matrix = np.array(
            [
                [
                    int(((predictions_df['y_true'] == 0) & (predictions_df['y_pred'] == 0)).sum()),
                    int(((predictions_df['y_true'] == 0) & (predictions_df['y_pred'] == 1)).sum()),
                ],
                [
                    int(((predictions_df['y_true'] == 1) & (predictions_df['y_pred'] == 0)).sum()),
                    int(((predictions_df['y_true'] == 1) & (predictions_df['y_pred'] == 1)).sum()),
                ],
            ]
        )

        plt.figure(figsize=(5, 4))
        plt.imshow(conf_matrix, interpolation='nearest', cmap='Blues')
        plt.title('Confusion Matrix (Autoencoder)')
        plt.colorbar()
        tick_marks = np.arange(2)
        plt.xticks(tick_marks, ['Pred 0', 'Pred 1'])
        plt.yticks(tick_marks, ['True 0', 'True 1'])
        for i in range(conf_matrix.shape[0]):
            for j in range(conf_matrix.shape[1]):
                plt.text(j, i, conf_matrix[i, j], ha='center', va='center', color='black')
        plt.ylabel('True label')
        plt.xlabel('Predicted label')
        plt.tight_layout()
        conf_path = tmp_path / 'confusion_matrix_test.png'
        plt.savefig(conf_path)
        plt.close()

        last_timestamp = predictions_df['timestamp'].max()
        window_start = last_timestamp - pd.Timedelta(days=2)
        last_2d_df = predictions_df[predictions_df['timestamp'] >= window_start].copy()
        last_2d_anomalies = last_2d_df[last_2d_df['y_pred'] == 1]

        plt.figure(figsize=(12, 4))
        plt.plot(last_2d_df['timestamp'], last_2d_df['reconstruction_error'], label='reconstruction_error', linewidth=1)
        if 'threshold' in last_2d_df.columns and len(last_2d_df) > 0:
            plt.axhline(float(last_2d_df['threshold'].iloc[0]), color='red', linestyle='--', label='threshold')
        plt.scatter(last_2d_anomalies['timestamp'], last_2d_anomalies['reconstruction_error'], color='orange', s=20, label='predicted_anomalies')
        plt.title('Predicted Anomalies Last 2 Days (Test)')
        plt.xlabel('timestamp')
        plt.ylabel('reconstruction_error')
        plt.legend()
        plt.tight_layout()
        last2d_path = tmp_path / 'predicted_anomalies_last_2days.png'
        plt.savefig(last2d_path)
        plt.close()

        mlflow.log_artifact(str(error_hist_path), artifact_path='diagnostics')
        mlflow.log_artifact(str(conf_path), artifact_path='diagnostics')
        mlflow.log_artifact(str(last2d_path), artifact_path='diagnostics')


def log_model_artifact(model, input_example: pd.DataFrame):
    model_device = next(model.parameters()).device
    input_tensor = torch.tensor(input_example.values, dtype=torch.float32).to(model_device)
    model_output_example = model(input_tensor).detach().cpu().numpy()
    signature = infer_signature(input_example, model_output_example)
    mlflow.pytorch.log_model(
        pytorch_model=model.cpu(),
        artifact_path='model',
        signature=signature,
        input_example=input_example,
    )


def log_inverter_run(result):
    inverter_id = result['inverter_id']

    with mlflow.start_run(run_name=f"autoencoder_optuna_inverter_{inverter_id}", nested=True):
        mlflow.set_tags(
            {
                'model_family': 'Autoencoder',
                'task': 'anomaly_detection',
                'time_series': 'true',
                'seed': str(SEED),
                'git_commit': get_git_commit(),
            }
        )

        mlflow.log_param('inverter_id', inverter_id)
        mlflow.log_param('model', 'Autoencoder')
        mlflow.log_param('train_perc', TRAIN_PERC)
        mlflow.log_param('n_splits', N_SPLITS)
        mlflow.log_param('n_trials', N_TRIALS)
        mlflow.log_param('target_col', TARGET_COL)
        mlflow.log_param('best_trial_number', result['best_trial_number'])
        mlflow.log_params(result['best_params'])

        mlflow.log_metric('cv_best_f1', result['best_cv_f1'])
        mlflow.log_metric('cv_best_objective', result['best_cv_objective'])
        mlflow.log_metric('n_rows', result['n_rows'])
        mlflow.log_metric('n_features', result['n_features'])
        mlflow.log_metric('n_train', result['n_train'])
        mlflow.log_metric('n_test', result['n_test'])
        mlflow.log_metric('threshold', result['threshold'])
        mlflow.log_metric('err_normal_mean', result['err_normal_mean'])
        mlflow.log_metric('err_anomaly_mean', result['err_anomaly_mean'])
        mlflow.log_metric('na_rows_removed', result['preprocessing_info']['na_rows_removed'])

        preprocessing_info = result['preprocessing_info']
        mlflow.log_param('dropped_columns', ','.join(preprocessing_info['dropped_columns']))
        mlflow.log_param('feature_count_final', len(preprocessing_info['feature_columns']))
        mlflow.log_param('train_start', preprocessing_info['train_start'])
        mlflow.log_param('train_end', preprocessing_info['train_end'])
        mlflow.log_param('test_start', preprocessing_info['test_start'])
        mlflow.log_param('test_end', preprocessing_info['test_end'])

        mlflow.log_dict(
            {
                'dropped_columns': preprocessing_info['dropped_columns'],
                'feature_columns': preprocessing_info['feature_columns'],
            },
            'preprocessing_snapshot.json',
        )

        for metric_name, metric_value in result['train_metrics'].items():
            mlflow.log_metric(f"train_{metric_name}", metric_value)

        for metric_name, metric_value in result['test_metrics'].items():
            mlflow.log_metric(f"test_{metric_name}", metric_value)

        scaler_mean_df = result['scaler_stats']['mean'].reset_index()
        scaler_mean_df.columns = ['feature', 'mean']
        scaler_std_df = result['scaler_stats']['std'].reset_index()
        scaler_std_df.columns = ['feature', 'std']

        log_dataframe_artifact(result['predictions_df'], 'test_predictions.csv', 'predictions')
        log_dataframe_artifact(result['top_trials_df'], 'optuna_top_trials.csv', 'optuna')
        log_dataframe_artifact(scaler_mean_df, 'scaler_mean.csv', 'preprocessing')
        log_dataframe_artifact(scaler_std_df, 'scaler_std.csv', 'preprocessing')
        log_diagnostic_plots(result['predictions_df'])
        log_model_artifact(result['model'], result['input_example'])


def run_training():
    data, data_meta = load_data()
    train_df = data['train']
    inverters = sorted(train_df['inverter_id'].dropna().astype(str).unique().tolist())

    if not inverters:
        raise ValueError('No inverters available after filtering')

    print(f"Training across {len(inverters)} inverter(s): {inverters}")
    all_results = []

    with mlflow.start_run(run_name='autoencoder_optuna_multi_inverter'):
        mlflow.set_tags(
            {
                'model_family': 'Autoencoder',
                'task': 'anomaly_detection',
                'time_series': 'true',
                'seed': str(SEED),
                'git_commit': get_git_commit(),
            }
        )

        mlflow.log_param('seed', SEED)
        mlflow.log_param('target_col', TARGET_COL)
        mlflow.log_param('selected_inverters', ','.join(inverters))
        mlflow.log_metric('train_rows', data_meta['train_rows'])
        mlflow.log_metric('test_rows', data_meta['test_rows'])
        mlflow.log_metric('dataset_cols', data_meta['dataset_cols'])
        mlflow.log_param('dataset_min_timestamp', data_meta['dataset_min_timestamp'])
        mlflow.log_param('dataset_max_timestamp', data_meta['dataset_max_timestamp'])
        mlflow.log_param('dataset_fingerprint_sha256', data_meta['dataset_fingerprint_sha256'])

        for inverter_id in inverters:
            try:
                print(f"\n=== Inverter {inverter_id} ===")
                result = train_for_inverter(data, inverter_id)
                all_results.append(result)
                log_inverter_run(result)

                metrics = result['test_metrics']
                print(
                    f"Test -> Precision: {metrics['precision']:.4f}, "
                    f"Recall: {metrics['recall']:.4f}, F1: {metrics['f1']:.4f}"
                )
            except Exception as exc:
                print(f"Skipping inverter {inverter_id}: {exc}")
                mlflow.log_text(
                    str(exc),
                    f"failures/inverter_{inverter_id}.txt",
                )

        if not all_results:
            raise RuntimeError('All inverters failed during training')

        mean_precision = float(np.mean([r['test_metrics']['precision'] for r in all_results]))
        mean_recall = float(np.mean([r['test_metrics']['recall'] for r in all_results]))
        mean_f1 = float(np.mean([r['test_metrics']['f1'] for r in all_results]))

        best_result = max(all_results, key=lambda x: x['test_metrics']['f1'])

        mlflow.log_metric('mean_test_precision', mean_precision)
        mlflow.log_metric('mean_test_recall', mean_recall)
        mlflow.log_metric('mean_test_f1', mean_f1)
        mlflow.log_param('best_inverter_by_f1', best_result['inverter_id'])

        print('\n=== Global Summary ===')
        print(f"Mean Test Precision: {mean_precision:.4f}")
        print(f"Mean Test Recall   : {mean_recall:.4f}")
        print(f"Mean Test F1       : {mean_f1:.4f}")
        print(
            f"Best inverter by F1: {best_result['inverter_id']} "
            f"({best_result['test_metrics']['f1']:.4f})"
        )


if __name__ == "__main__":
    initialize_mlflow_config()
    run_training()