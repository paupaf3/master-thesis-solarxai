import mlflow
import mlflow.sklearn
import os
import json


EXPERIMENT_NAME = "Time Series Forecasting using ML GradientBoostingRegressor"


def get_candidate_runs(cfg, inverter_id):
    mlflow.set_tracking_uri(cfg.mlflow_uri)
    experiment = mlflow.get_experiment_by_name(EXPERIMENT_NAME)
    if experiment is None:
        raise RuntimeError(f"Experiment '{EXPERIMENT_NAME}' not found in MLflow.")

    runs = mlflow.search_runs(
        experiment_ids=[experiment.experiment_id],
        filter_string=(
            f"params.inverter_id = '{inverter_id}'"
            " and params.model = 'GradientBoostingRegressor'"
            " and attributes.status = 'FINISHED'"
        ),
        order_by=["metrics.test_mae ASC"],
    )
    if runs.empty:
        raise RuntimeError(f"No completed runs found for inverter {inverter_id}.")
    return runs


def load_model(run):
    run_id = run.run_id
    model_uri = f"runs:/{run_id}/model"
    model = mlflow.sklearn.load_model(model_uri)
    local_dir = mlflow.artifacts.download_artifacts(run_id=run_id, artifact_path="", dst_path=None)
    return model, local_dir


def load_preprocessing(local_dir):
    with open(os.path.join(local_dir, "preprocessing_snapshot.json"), "r") as f:
        feature_snapshot = json.load(f)
    preprocessing = {
        "feature_snapshot": feature_snapshot,
    }
    return preprocessing


def load_model_and_preprocessing(cfg, inverter_id):
    candidates = get_candidate_runs(cfg, inverter_id)
    last_exc = None
    for _, run in candidates.iterrows():
        try:
            model, local_dir = load_model(run)
            preprocessing = load_preprocessing(local_dir)
            return run, model, preprocessing
        except Exception as exc:
            last_exc = exc
            continue
    raise RuntimeError(
        f"All {len(candidates)} runs failed for inverter {inverter_id}. "
        f"Last error: {last_exc}"
    )