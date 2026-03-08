import psycopg2
from psycopg2.extras import execute_values


def write_predictions(cfg, inverter_id, df, predictions, run_info):
    """
    Write forecasted predictions to the database.
    
    Args:
        cfg: Configuration object
        inverter_id: Inverter identifier
        df: DataFrame with 'plant_id', 'inverter_id', 'timestamp' columns
            (timestamps are FUTURE timestamps for which we are predicting)
        predictions: Array of predicted ac_power_kw values
        run_info: MLflow run information
    """
    conn = psycopg2.connect(
        host=cfg.db_host,
        port=cfg.db_port,
        dbname=cfg.db_name,
        user=cfg.db_user,
        password=cfg.db_password,
    )
    cur = conn.cursor()

    run_id = run_info.run_id
    records = [
        (
            str(df.iloc[i]["plant_id"]),
            inverter_id,
            df.iloc[i]["timestamp"],
            run_id,
            float(predictions[i]),
        )
        for i in range(len(df))
    ]

    insert_query = """
        INSERT INTO gold.forecast_predictions
            (plant_id, inverter_id, timestamp, model_run_id, predicted_ac_power_kw)
        VALUES %s
        ON CONFLICT (plant_id, inverter_id, timestamp, model_run_id) DO UPDATE
        SET predicted_ac_power_kw = EXCLUDED.predicted_ac_power_kw,
            created_at = CURRENT_TIMESTAMP
    """
    execute_values(cur, insert_query, records)

    conn.commit()
    cur.close()
    conn.close()