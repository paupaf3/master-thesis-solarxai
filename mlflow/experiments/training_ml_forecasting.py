import mlflow
import optuna
import data_loader as dl
import numpy as np
import pandas as pd
import hashlib
import matplotlib.pyplot as plt
import subprocess
import tempfile
from pathlib import Path

from sklearn.metrics import mean_squared_error, mean_absolute_error
from sklearn.model_selection import TimeSeriesSplit
from sklearn.ensemble import GradientBoostingRegressor
from mlflow.models import infer_signature

SEED = 42
INVERTER_IDS = None
TARGET_COL = 'ac_power_kw'
ID_COLS = ['inverter_id', 'state']
TRAIN_PERC = 0.8
N_SPLITS = 5
N_TRIALS = 10

LEAKAGE_COLS = [
    'inverter_temp_c', 'ac_freq_hz', 'dc_power_kw', 'dc_voltage_v', 'dc_current_a',
    'active_failures', 'healthy_strings', 'failed_strings',
    'amb_temp_c', 'module_temp_c', 'wind_speed_ms', 'wind_dir_deg',
    'humidity_percent', 'poa_irradiance_wm2',
    'dc_to_ac_ratio', 'power_per_healthy_string', 'temp_diff',
    'irradiance_temp_interaction',
]


def initialize_mlflow_config():
    mlflow.set_experiment("Time Series Forecasting using ML GradientBoostingRegressor")
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
    data = dl.ml_data_loader()

    required_cols = {'timestamp', 'inverter_id', TARGET_COL}
    missing_required = required_cols - set(data.columns)
    if missing_required:
        missing_as_text = ", ".join(sorted(missing_required))
        raise ValueError(f"Dataset is missing required columns: {missing_as_text}")

    if INVERTER_IDS:
        data = data[data['inverter_id'].isin(INVERTER_IDS)].copy()

    data = data.sort_values(['inverter_id', 'timestamp']).reset_index(drop=True)
    data_meta = {
        'dataset_rows': int(len(data)),
        'dataset_cols': int(len(data.columns)),
        'dataset_min_timestamp': to_iso_timestamp(data['timestamp'].min()),
        'dataset_max_timestamp': to_iso_timestamp(data['timestamp'].max()),
        'dataset_fingerprint_sha256': compute_data_fingerprint(data),
    }
    return data, data_meta


def prepare_inverter_data(data: pd.DataFrame, inverter_id: str):
    inverter_df = data[data['inverter_id'].astype(str) == str(inverter_id)].copy()
    inverter_df = inverter_df.set_index('timestamp').sort_index()

    drop_cols = [c for c in (ID_COLS + LEAKAGE_COLS) if c in inverter_df.columns]
    model_df = inverter_df.drop(columns=drop_cols)
    rows_before_dropna = len(model_df)
    model_df = model_df.dropna()
    rows_after_dropna = len(model_df)
    na_rows_removed = rows_before_dropna - rows_after_dropna

    if TARGET_COL not in model_df.columns:
        raise ValueError(f"Target column '{TARGET_COL}' not found after preprocessing for inverter {inverter_id}")

    if len(model_df) < 100:
        raise ValueError(f"Not enough rows for inverter {inverter_id}: {len(model_df)} rows after preprocessing")

    X = model_df.drop(columns=[TARGET_COL])
    y = model_df[TARGET_COL]
    feature_columns = X.columns.tolist()

    train_size = int(len(X) * TRAIN_PERC)
    X_train, X_test = X.iloc[:train_size], X.iloc[train_size:]
    y_train, y_test = y.iloc[:train_size], y.iloc[train_size:]

    if len(X_train) <= N_SPLITS:
        raise ValueError(
            f"Training set too small for TimeSeriesSplit(n_splits={N_SPLITS}) "
            f"for inverter {inverter_id}: {len(X_train)} rows"
        )

    preprocessing_info = {
        'dropped_columns': drop_cols,
        'feature_columns': feature_columns,
        'na_rows_removed': int(na_rows_removed),
        'train_start': to_iso_timestamp(X_train.index.min()),
        'train_end': to_iso_timestamp(X_train.index.max()),
        'test_start': to_iso_timestamp(X_test.index.min()),
        'test_end': to_iso_timestamp(X_test.index.max()),
    }

    return X_train, X_test, y_train, y_test, preprocessing_info


def calculate_metrics(y_true, y_pred):
    y_pred = np.clip(y_pred, 0, None)
    mse = mean_squared_error(y_true, y_pred)
    mae = mean_absolute_error(y_true, y_pred)
    rmse = float(np.sqrt(mse))

    return {
        'mse': float(mse),
        'mae': float(mae),
        'rmse': rmse,
    }


def evaluate_model(model, X_val, y_val):
    y_pred = model.predict(X_val)
    return calculate_metrics(y_val, y_pred)


def objective(trial, X_train, y_train):
    params = {
        'n_estimators': trial.suggest_int('n_estimators', 150, 1200),
        'learning_rate': trial.suggest_float('learning_rate', 1e-3, 0.2, log=True),
        'max_depth': trial.suggest_int('max_depth', 2, 8),
        'subsample': trial.suggest_float('subsample', 0.5, 1.0),
        'min_samples_split': trial.suggest_int('min_samples_split', 2, 40),
        'min_samples_leaf': trial.suggest_int('min_samples_leaf', 1, 20),
        'max_features': trial.suggest_categorical('max_features', ['sqrt', 'log2', None]),
        'loss': trial.suggest_categorical('loss', ['squared_error', 'absolute_error', 'huber']),
        'random_state': SEED,
    }

    tscv = TimeSeriesSplit(n_splits=N_SPLITS)
    fold_mae = []

    for train_idx, val_idx in tscv.split(X_train):
        X_fold_train, X_fold_val = X_train.iloc[train_idx], X_train.iloc[val_idx]
        y_fold_train, y_fold_val = y_train.iloc[train_idx], y_train.iloc[val_idx]

        model = GradientBoostingRegressor(**params)
        model.fit(X_fold_train, y_fold_train)

        metrics = evaluate_model(model, X_fold_val, y_fold_val)
        fold_mae.append(metrics['mae'])

    return float(np.mean(fold_mae))


def tune_hyperparameters(X_train, y_train):
    sampler = optuna.samplers.TPESampler(seed=SEED)
    pruner = optuna.pruners.MedianPruner(n_startup_trials=5)
    study = optuna.create_study(direction='minimize', sampler=sampler, pruner=pruner)

    study.optimize(lambda trial: objective(trial, X_train, y_train), n_trials=N_TRIALS)
    return study


def train_for_inverter(data: pd.DataFrame, inverter_id: str):
    X_train, X_test, y_train, y_test, preprocessing_info = prepare_inverter_data(data, inverter_id)

    study = tune_hyperparameters(X_train, y_train)
    best_params = {**study.best_params, 'random_state': SEED}

    model = GradientBoostingRegressor(**best_params)
    model.fit(X_train, y_train)

    y_pred_test = np.clip(model.predict(X_test), 0, None)

    train_metrics = evaluate_model(model, X_train, y_train)
    test_metrics = calculate_metrics(y_test, y_pred_test)

    predictions_df = pd.DataFrame(
        {
            'timestamp': y_test.index,
            'y_true': y_test.values,
            'y_pred': y_pred_test,
        }
    )
    predictions_df['abs_error'] = (predictions_df['y_true'] - predictions_df['y_pred']).abs()

    trials_df = study.trials_dataframe(attrs=('number', 'value', 'state', 'params'))
    trials_df = trials_df[trials_df['state'] == 'COMPLETE'].sort_values('value', ascending=True).head(5)

    input_example = X_train.head(min(5, len(X_train))).copy()

    return {
        'inverter_id': inverter_id,
        'n_rows': int(len(X_train) + len(X_test)),
        'n_features': int(X_train.shape[1]),
        'n_train': int(len(X_train)),
        'n_test': int(len(X_test)),
        'best_params': best_params,
        'best_cv_mae': float(study.best_value),
        'train_metrics': train_metrics,
        'test_metrics': test_metrics,
        'preprocessing_info': preprocessing_info,
        'predictions_df': predictions_df,
        'top_trials_df': trials_df,
        'best_trial_number': int(study.best_trial.number),
        'model': model,
        'input_example': input_example,
    }


def log_dataframe_artifact(df: pd.DataFrame, filename: str, artifact_path: str):
    with tempfile.TemporaryDirectory() as tmp_dir:
        file_path = Path(tmp_dir) / filename
        df.to_csv(file_path, index=False)
        mlflow.log_artifact(str(file_path), artifact_path=artifact_path)


def log_diagnostic_plots(predictions_df: pd.DataFrame):
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)

        plt.figure(figsize=(12, 4))
        plt.plot(predictions_df['timestamp'], predictions_df['y_true'], label='actual')
        plt.plot(predictions_df['timestamp'], predictions_df['y_pred'], label='predicted')
        plt.title('Actual vs Predicted (Test)')
        plt.xlabel('timestamp')
        plt.ylabel(TARGET_COL)
        plt.legend()
        plt.tight_layout()
        actual_vs_pred_path = tmp_path / 'actual_vs_pred_test.png'
        plt.savefig(actual_vs_pred_path)
        plt.close()

        residuals = predictions_df['y_true'] - predictions_df['y_pred']
        plt.figure(figsize=(8, 4))
        plt.hist(residuals, bins=30)
        plt.title('Residual Distribution (Test)')
        plt.xlabel('residual')
        plt.ylabel('count')
        plt.tight_layout()
        residuals_path = tmp_path / 'residuals_hist_test.png'
        plt.savefig(residuals_path)
        plt.close()

        mlflow.log_artifact(str(actual_vs_pred_path), artifact_path='diagnostics')
        mlflow.log_artifact(str(residuals_path), artifact_path='diagnostics')


def log_model_artifact(model, input_example: pd.DataFrame):
    model_output_example = np.clip(model.predict(input_example), 0, None)
    signature = infer_signature(input_example, model_output_example)
    mlflow.sklearn.log_model(
        sk_model=model,
        artifact_path='model',
        signature=signature,
        input_example=input_example,
    )


def log_inverter_run(result):
    inverter_id = result['inverter_id']

    with mlflow.start_run(run_name=f"gbr_optuna_inverter_{inverter_id}", nested=True):
        mlflow.set_tags(
            {
                'model_family': 'GradientBoostingRegressor',
                'task': 'time_series_forecasting',
                'time_series': 'true',
                'seed': str(SEED),
                'git_commit': get_git_commit(),
            }
        )

        mlflow.log_param('inverter_id', inverter_id)
        mlflow.log_param('model', 'GradientBoostingRegressor')
        mlflow.log_param('train_perc', TRAIN_PERC)
        mlflow.log_param('n_splits', N_SPLITS)
        mlflow.log_param('n_trials', N_TRIALS)
        mlflow.log_param('best_trial_number', result['best_trial_number'])
        mlflow.log_params(result['best_params'])

        mlflow.log_metric('cv_best_mae', result['best_cv_mae'])
        mlflow.log_metric('n_rows', result['n_rows'])
        mlflow.log_metric('n_features', result['n_features'])
        mlflow.log_metric('n_train', result['n_train'])
        mlflow.log_metric('n_test', result['n_test'])
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

        log_dataframe_artifact(result['predictions_df'], 'test_predictions.csv', 'predictions')
        log_dataframe_artifact(result['top_trials_df'], 'optuna_top_trials.csv', 'optuna')
        log_diagnostic_plots(result['predictions_df'])
        log_model_artifact(result['model'], result['input_example'])


def run_training():
    data, data_meta = load_data()
    inverters = sorted(data['inverter_id'].dropna().astype(str).unique().tolist())

    if not inverters:
        raise ValueError("No inverters available after filtering")

    print(f"Training across {len(inverters)} inverter(s): {inverters}")
    all_results = []

    with mlflow.start_run(run_name='gbr_optuna_multi_inverter'):
        mlflow.set_tags(
            {
                'model_family': 'GradientBoostingRegressor',
                'task': 'time_series_forecasting',
                'time_series': 'true',
                'seed': str(SEED),
                'git_commit': get_git_commit(),
            }
        )

        mlflow.log_param('seed', SEED)
        mlflow.log_param('target_col', TARGET_COL)
        mlflow.log_param('selected_inverters', ','.join(inverters))
        mlflow.log_metric('dataset_rows', data_meta['dataset_rows'])
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
                    f"Test -> MAE: {metrics['mae']:.4f}, RMSE: {metrics['rmse']:.4f}"
                )
            except Exception as exc:
                print(f"Skipping inverter {inverter_id}: {exc}")
                mlflow.log_text(
                    str(exc),
                    f"failures/inverter_{inverter_id}.txt",
                )

        if not all_results:
            raise RuntimeError("All inverters failed during training")

        mean_mae = float(np.mean([r['test_metrics']['mae'] for r in all_results]))
        mean_rmse = float(np.mean([r['test_metrics']['rmse'] for r in all_results]))

        best_result = min(all_results, key=lambda x: x['test_metrics']['mae'])

        mlflow.log_metric('mean_test_mae', mean_mae)
        mlflow.log_metric('mean_test_rmse', mean_rmse)
        mlflow.log_param('best_inverter_by_mae', best_result['inverter_id'])

        print("\n=== Global Summary ===")
        print(f"Mean Test MAE : {mean_mae:.4f}")
        print(f"Mean Test RMSE: {mean_rmse:.4f}")
        print(
            f"Best inverter by MAE: {best_result['inverter_id']} "
            f"({best_result['test_metrics']['mae']:.4f})"
        )


if __name__ == "__main__":
    initialize_mlflow_config()
    run_training()
