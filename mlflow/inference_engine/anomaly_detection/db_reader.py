import pandas as pd
from sqlalchemy import create_engine


def _make_engine(cfg):
    url = f"postgresql+psycopg2://{cfg.db_user}:{cfg.db_password}@{cfg.db_host}:{cfg.db_port}/{cfg.db_name}"
    return create_engine(url)


def get_last_processed_ts(engine, inverter_id):
    query = """
        SELECT last_processed_ts
        FROM gold.inference_watermark
        WHERE inverter_id = %(inv)s AND inference_mode = 'anomaly'
    """
    result = pd.read_sql(query, engine, params={"inv": inverter_id})
    if result.empty:
        return None
    return result.iloc[0]['last_processed_ts']


def fetch_new_rows(cfg, inverter_id):
    engine = _make_engine(cfg)
    last_ts = get_last_processed_ts(engine, inverter_id)

    base_query = """
        SELECT
            inv.plant_id,
            inv.inverter_id,
            inv.timestamp,
            inv.inverter_temp_c,
            inv.ac_power_kw,
            inv.ac_freq_hz,
            inv.dc_power_kw,
            inv.dc_voltage_v,
            inv.dc_current_a,
            inv.healthy_strings,
            inv.failed_strings,
            inv.active_failures,
            met.amb_temp_c,
            met.module_temp_c,
            met.wind_speed_ms,
            met.wind_dir_deg,
            met.humidity_percent,
            met.poa_irradiance_wm2
        FROM silver.inverter_cleaned inv
        LEFT JOIN silver.meteo_station_cleaned met
            ON inv.plant_id = met.plant_id
            AND inv.timestamp = met.timestamp
        WHERE inv.inverter_id = %(inv)s
    """

    if last_ts is not None:
        query = base_query + "    AND inv.timestamp > %(last_ts)s\n        ORDER BY inv.timestamp ASC"
        df = pd.read_sql(query, engine, params={"inv": inverter_id, "last_ts": last_ts})
    else:
        query = base_query + "    ORDER BY inv.timestamp ASC"
        df = pd.read_sql(query, engine, params={"inv": inverter_id})

    engine.dispose()
    return df