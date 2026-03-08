import argparse
import time
import json
import psycopg2
from kafka import KafkaConsumer
import config


def discover_inverter_ids(cfg):
    """Query the DB for all known inverter IDs from the silver layer."""
    conn = psycopg2.connect(
        host=cfg.db_host,
        port=cfg.db_port,
        dbname=cfg.db_name,
        user=cfg.db_user,
        password=cfg.db_password,
    )
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT inverter_id FROM silver.inverter_cleaned ORDER BY inverter_id")
    ids = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return ids


def main():
    parser = argparse.ArgumentParser(description="Inference Engine")
    parser.add_argument(
        "mode",
        choices=["anomaly", "forecasting"],
        help="Which inference pipeline to run"
    )
    args = parser.parse_args()
    cfg = config.load_config()

    if args.mode == "anomaly":
        from anomaly_detection.orchestrator import run as run_pipeline
    elif args.mode == "forecasting":
        from forecasting.orchestrator import run as run_pipeline

    group_id = f"inference-{args.mode}-group"
    consumer = KafkaConsumer(
        cfg.trigger_topic,
        bootstrap_servers=cfg.kafka_brokers.split(","),
        group_id=group_id,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=1000,  # poll returns after 1s if no messages
    )

    print(f"[inference] Starting {args.mode} engine "
          f"(topic={cfg.trigger_topic}, group={group_id}, window={cfg.window_seconds}s)")

    triggers_received = 0
    last_run_time = time.time()

    while True:
        # Poll Kafka for trigger messages (non-blocking, 1s timeout)
        for msg in consumer:
            event = msg.value
            data_type = event.get("data_type", "")
            count = event.get("count", 0)
            triggers_received += count
            print(f"[inference] Trigger: {data_type} +{count} rows "
                  f"(accumulated={triggers_received})")

        elapsed = time.time() - last_run_time

        # Run inference when window has elapsed AND we have accumulated triggers
        if elapsed >= cfg.window_seconds and triggers_received > 0:
            print(f"[inference] Window elapsed ({elapsed:.0f}s), "
                  f"{triggers_received} new rows — running {args.mode} inference...")
            try:
                if not cfg.inverter_ids:
                    cfg.inverter_ids = discover_inverter_ids(cfg)
                    print(f"[inference] Discovered {len(cfg.inverter_ids)} inverter(s): "
                          f"{cfg.inverter_ids}")

                if cfg.inverter_ids:
                    run_pipeline(cfg)
                else:
                    print("[inference] No inverters found in silver layer.")
            except Exception as exc:
                print(f"[inference] Error during {args.mode} cycle: {exc}")

            triggers_received = 0
            last_run_time = time.time()

        # Brief sleep to avoid busy-waiting between Kafka polls
        time.sleep(10)


if __name__ == "__main__":
    main()