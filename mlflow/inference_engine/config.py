import os

class Config:
    def __init__(self):
        self.mlflow_uri = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow-server:5000")
        self.db_host = os.getenv("DB_HOST", "localhost")
        self.db_port = int(os.getenv("DB_PORT", "5432"))
        self.db_name = os.getenv("DB_NAME", "solarxai")
        self.db_user = os.getenv("DB_USER", "postgres")
        self.db_password = os.getenv("DB_PASSWORD", "")
        self.device = os.getenv("DEVICE", "cpu")
        self.inverter_ids = os.getenv("INVERTER_IDS", "").split(",") if os.getenv("INVERTER_IDS") else None
        # Forecasting settings
        self.forecast_horizon_hours = int(os.getenv("FORECAST_HORIZON_HOURS", "48"))
        self.forecast_frequency_minutes = int(os.getenv("FORECAST_FREQUENCY_MINUTES", "15"))
        # Kafka settings
        self.kafka_brokers = os.getenv("KAFKA_BROKERS", "broker-1:29092,broker-2:29192,broker-3:29292")
        self.trigger_topic = os.getenv("INFERENCE_TRIGGER_TOPIC", "inference-trigger")
        self.window_seconds = int(os.getenv("INFERENCE_WINDOW_SECONDS", "300"))

def load_config():
    return Config()