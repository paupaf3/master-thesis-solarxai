import mlflow
import pandas as pd
import os
import json
import torch


EXPERIMENT_NAME = "Inverter Anomaly Detection using DL Autoencoder"


class Autoencoder(torch.nn.Module):
    """Autoencoder model for anomaly detection.
    
    Defined locally to avoid Python version bytecode incompatibility when
    loading models trained with a different Python version.
    """

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


def get_candidate_runs(cfg, inverter_id):
    mlflow.set_tracking_uri(cfg.mlflow_uri)
    experiment = mlflow.get_experiment_by_name(EXPERIMENT_NAME)
    if experiment is None:
        raise RuntimeError(f"Experiment '{EXPERIMENT_NAME}' not found in MLflow.")

    runs = mlflow.search_runs(
        experiment_ids=[experiment.experiment_id],
        filter_string=(
            f"params.inverter_id = '{inverter_id}'"
            " and params.model = 'Autoencoder'"
            " and attributes.status = 'FINISHED'"
        ),
        order_by=["metrics.test_f1 DESC"],
    )
    if runs.empty:
        raise RuntimeError(f"No completed runs found for inverter {inverter_id}.")
    return runs


def load_model(run):
    """Load model by extracting state_dict and creating fresh model instance.
    
    This approach avoids Python bytecode incompatibility issues when loading
    models trained with a different Python version.
    """
    run_id = run.run_id
    
    # Download model artifact specifically (models are stored separately in MLflow 3.x)
    model_dir = mlflow.artifacts.download_artifacts(run_id=run_id, artifact_path="model", dst_path=None)
    # Download other artifacts (preprocessing) separately
    artifacts_dir = mlflow.artifacts.download_artifacts(run_id=run_id, artifact_path="", dst_path=None)
    
    # Load model weights by extracting state_dict from pickled model
    model_path = os.path.join(model_dir, "data", "model.pth")
    pickled_model = torch.load(model_path, weights_only=False)
    state_dict = pickled_model.state_dict()
    
    # Infer model dimensions from weight shapes
    input_dim = state_dict["encoder.0.weight"].shape[1]
    hidden_dim = state_dict["encoder.0.weight"].shape[0]
    bottleneck_dim = state_dict["encoder.2.weight"].shape[0]
    
    # Create fresh model in current Python environment and load weights
    model = Autoencoder(input_dim, hidden_dim, bottleneck_dim)
    model.load_state_dict(state_dict)
    
    return model, artifacts_dir


def load_preprocessing(local_dir, run):
    with open(os.path.join(local_dir, "preprocessing_snapshot.json"), "r") as f:
        feature_snapshot = json.load(f)

    scaler_mean_df = pd.read_csv(os.path.join(local_dir, "preprocessing", "scaler_mean.csv"))
    scaler_mean = scaler_mean_df.set_index("feature")["mean"]

    scaler_std_df = pd.read_csv(os.path.join(local_dir, "preprocessing", "scaler_std.csv"))
    scaler_std = scaler_std_df.set_index("feature")["std"]
    scaler_std = scaler_std.replace(0, 1.0)  # avoid division by zero

    threshold = float(run.get("metrics.threshold", 0.0))

    preprocessing = {
        "feature_snapshot": feature_snapshot,
        "scaler_mean": scaler_mean,
        "scaler_std": scaler_std,
        "threshold": threshold,
    }
    return preprocessing


def load_model_and_preprocessing(cfg, inverter_id):
    candidates = get_candidate_runs(cfg, inverter_id)
    last_exc = None
    for _, run in candidates.iterrows():
        try:
            model, local_dir = load_model(run)
            preprocessing = load_preprocessing(local_dir, run)
            return run, model, preprocessing
        except Exception as exc:
            last_exc = exc
            continue
    raise RuntimeError(
        f"All {len(candidates)} runs failed for inverter {inverter_id}. "
        f"Last error: {last_exc}"
    )