import psycopg2
from psycopg2.extras import execute_values


def write_predictions(cfg, inverter_id, df, predictions, run_info):
    conn = psycopg2.connect(
        host=cfg.db_host,
        port=cfg.db_port,
        dbname=cfg.db_name,
        user=cfg.db_user,
        password=cfg.db_password,
    )
    cur = conn.cursor()

    run_id = run_info.run_id
    threshold = predictions["threshold"]
    records = [
        (
            str(df.iloc[i]["plant_id"]),
            inverter_id,
            df.iloc[i]["timestamp"],
            run_id,
            float(predictions["reconstruction_errors"][i]),
            float(threshold),
            int(predictions["is_anomaly"][i]),
        )
        for i in range(len(df))
    ]

    insert_query = """
        INSERT INTO gold.anomaly_predictions
            (plant_id, inverter_id, timestamp, model_run_id, reconstruction_error, threshold, is_anomaly)
        VALUES %s
        ON CONFLICT (plant_id, inverter_id, timestamp, model_run_id) DO NOTHING
    """
    execute_values(cur, insert_query, records)

    update_watermark(cur, inverter_id, df["timestamp"].max())

    conn.commit()
    cur.close()
    conn.close()


def update_watermark(cur, inverter_id, last_ts):
    query = """
        INSERT INTO gold.inference_watermark (inverter_id, inference_mode, last_processed_ts, updated_at)
        VALUES (%s, 'anomaly', %s, NOW())
        ON CONFLICT (inverter_id, inference_mode)
        DO UPDATE SET last_processed_ts = EXCLUDED.last_processed_ts, updated_at = NOW()
    """
    cur.execute(query, (inverter_id, last_ts))