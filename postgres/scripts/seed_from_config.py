#!/usr/bin/env python3
"""
Plant Configuration Seeder for PostgreSQL

This script reads the plant_config.json file and seeds the dimension tables
in PostgreSQL. It's designed to be run during container initialization.

Usage:
    python seed_from_config.py [--config PATH] [--dry-run]

The script will:
1. Read plant_config.json from /config/plant_config.json or specified path
2. Connect to PostgreSQL using environment variables
3. Upsert data into silver.dim_plant_config, silver.dim_asset, and silver.dim_thresholds
"""

import json
import os
import sys
import argparse
from pathlib import Path
from typing import Dict, Any

# PostgreSQL connection
try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    print("Warning: psycopg2 not installed. Install with: pip install psycopg2-binary")
    psycopg2 = None


def get_config_path() -> Path:
    """Get the path to the plant configuration file"""
    if os.getenv("PLANT_CONFIG_PATH"):
        return Path(os.getenv("PLANT_CONFIG_PATH"))

    # Docker container path
    docker_path = Path("/config/plant_config.json")
    if docker_path.exists():
        return docker_path

    # Local development path
    script_dir = Path(__file__).parent
    local_path = script_dir.parent.parent / "config" / "plant_config.json"
    if local_path.exists():
        return local_path

    raise FileNotFoundError(f"Could not find plant_config.json")


def load_config(config_path: Path) -> Dict[str, Any]:
    """Load configuration from JSON file"""
    with open(config_path, 'r') as f:
        return json.load(f)


def get_db_connection():
    """Get PostgreSQL connection using environment variables"""
    if psycopg2 is None:
        raise ImportError("psycopg2 is required")

    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        database=os.getenv("DB_NAME", "solarxai_medallion"),
        user=os.getenv("DB_USER", "solarxai"),
        password=os.getenv("DB_PASSWORD", "solarxai_password_123")
    )


def seed_plant_config(conn, config: Dict[str, Any], dry_run: bool = False) -> None:
    """Seed the dim_plant_config table"""
    plant = config.get('plant', {})
    location = plant.get('location', {})
    grid = plant.get('grid_connection', {})

    sql = """
        INSERT INTO silver.dim_plant_config (
            plant_id, plant_name,
            latitude, longitude, timezone, altitude_m,
            total_capacity_kwp, total_surface_m2, total_inverters,
            grid_voltage_nominal_v, grid_frequency_nominal_hz, grid_phases,
            commissioning_date, config_version
        ) VALUES (
            %(plant_id)s::uuid, %(plant_name)s,
            %(latitude)s, %(longitude)s, %(timezone)s, %(altitude_m)s,
            %(total_capacity_kwp)s, %(total_surface_m2)s, %(total_inverters)s,
            %(grid_voltage_nominal_v)s, %(grid_frequency_nominal_hz)s, %(grid_phases)s,
            %(commissioning_date)s, %(config_version)s
        )
        ON CONFLICT (plant_id) DO UPDATE SET
            plant_name = EXCLUDED.plant_name,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            timezone = EXCLUDED.timezone,
            altitude_m = EXCLUDED.altitude_m,
            total_capacity_kwp = EXCLUDED.total_capacity_kwp,
            total_surface_m2 = EXCLUDED.total_surface_m2,
            total_inverters = EXCLUDED.total_inverters,
            grid_voltage_nominal_v = EXCLUDED.grid_voltage_nominal_v,
            grid_frequency_nominal_hz = EXCLUDED.grid_frequency_nominal_hz,
            grid_phases = EXCLUDED.grid_phases,
            commissioning_date = EXCLUDED.commissioning_date,
            config_version = EXCLUDED.config_version,
            updated_at = CURRENT_TIMESTAMP
    """

    # Calculate total inverters from groups
    total_inverters = sum(g['inverter_count']
                          for g in config.get('inverter_groups', []))

    params = {
        'plant_id': plant.get('plant_id'),
        'plant_name': plant.get('plant_name'),
        'latitude': location.get('latitude'),
        'longitude': location.get('longitude'),
        'timezone': location.get('timezone'),
        'altitude_m': location.get('altitude_m'),
        'total_capacity_kwp': plant.get('total_capacity_kwp'),
        'total_surface_m2': plant.get('total_surface_m2'),
        'total_inverters': total_inverters,
        'grid_voltage_nominal_v': grid.get('voltage_nominal_v'),
        'grid_frequency_nominal_hz': grid.get('frequency_nominal_hz'),
        'grid_phases': grid.get('phases'),
        'commissioning_date': plant.get('commissioning_date'),
        'config_version': config.get('version')
    }

    if dry_run:
        print(f"[DRY-RUN] Would insert plant config: {params['plant_name']}")
        return

    with conn.cursor() as cur:
        cur.execute(sql, params)

    print(f"Seeded plant config: {params['plant_name']}")


def seed_inverter_assets(conn, config: Dict[str, Any], dry_run: bool = False) -> None:
    """Seed inverter assets into dim_asset table"""
    plant_id = config.get('plant', {}).get('plant_id')
    inv_specs = config.get('inverter_specs', {})
    dc = inv_specs.get('dc_side', {})
    ac = inv_specs.get('ac_side', {})
    eff = inv_specs.get('efficiency', {})
    thermal = inv_specs.get('thermal', {})

    sql = """
        INSERT INTO silver.dim_asset (
            plant_id, asset_id, asset_type, asset_group,
            nominal_capacity_kw, panel_surface_m2,
            dc_voltage_mpp_min_v, dc_voltage_mpp_max_v, dc_voltage_nominal_v,
            dc_current_max_a, strings_count, panels_per_string, panel_wp,
            ac_voltage_nominal_v, ac_frequency_nominal_hz, power_factor_nominal,
            efficiency_dc_ac_nominal, efficiency_dc_ac_min_acceptable,
            efficiency_system_min, efficiency_system_max,
            temp_coefficient_pct_per_c, stc_temperature_c, max_operating_temp_c,
            is_active, commissioned_date
        ) VALUES (
            %(plant_id)s::uuid, %(asset_id)s, 'inverter', %(asset_group)s,
            %(nominal_capacity_kw)s, %(panel_surface_m2)s,
            %(dc_voltage_mpp_min_v)s, %(dc_voltage_mpp_max_v)s, %(dc_voltage_nominal_v)s,
            %(dc_current_max_a)s, %(strings_count)s, %(panels_per_string)s, %(panel_wp)s,
            %(ac_voltage_nominal_v)s, %(ac_frequency_nominal_hz)s, %(power_factor_nominal)s,
            %(efficiency_dc_ac_nominal)s, %(efficiency_dc_ac_min_acceptable)s,
            %(efficiency_system_min)s, %(efficiency_system_max)s,
            %(temp_coefficient_pct_per_c)s, %(stc_temperature_c)s, %(max_operating_temp_c)s,
            TRUE, %(commissioned_date)s
        )
        ON CONFLICT (plant_id, asset_id) DO UPDATE SET
            asset_group = EXCLUDED.asset_group,
            nominal_capacity_kw = EXCLUDED.nominal_capacity_kw,
            panel_surface_m2 = EXCLUDED.panel_surface_m2,
            dc_voltage_mpp_min_v = EXCLUDED.dc_voltage_mpp_min_v,
            dc_voltage_mpp_max_v = EXCLUDED.dc_voltage_mpp_max_v,
            dc_voltage_nominal_v = EXCLUDED.dc_voltage_nominal_v,
            dc_current_max_a = EXCLUDED.dc_current_max_a,
            strings_count = EXCLUDED.strings_count,
            panels_per_string = EXCLUDED.panels_per_string,
            panel_wp = EXCLUDED.panel_wp,
            efficiency_dc_ac_nominal = EXCLUDED.efficiency_dc_ac_nominal,
            efficiency_dc_ac_min_acceptable = EXCLUDED.efficiency_dc_ac_min_acceptable,
            efficiency_system_min = EXCLUDED.efficiency_system_min,
            efficiency_system_max = EXCLUDED.efficiency_system_max,
            updated_at = CURRENT_TIMESTAMP
    """

    eff_range = eff.get('system_efficiency_range', [0.88, 0.92])
    commissioned = config.get('plant', {}).get('commissioning_date')

    inverter_count = 0
    for group in config.get('inverter_groups', []):
        group_id = group['group_id']
        for inv_id in group['inverter_ids']:
            params = {
                'plant_id': plant_id,
                'asset_id': inv_id,
                'asset_group': group_id,
                'nominal_capacity_kw': inv_specs.get('nominal_capacity_kw'),
                'panel_surface_m2': inv_specs.get('panel_surface_m2'),
                'dc_voltage_mpp_min_v': dc.get('voltage_mpp_min_v'),
                'dc_voltage_mpp_max_v': dc.get('voltage_mpp_max_v'),
                'dc_voltage_nominal_v': dc.get('voltage_nominal_v'),
                'dc_current_max_a': dc.get('current_max_a'),
                'strings_count': dc.get('strings_count'),
                'panels_per_string': dc.get('panels_per_string'),
                'panel_wp': dc.get('panel_wp'),
                'ac_voltage_nominal_v': ac.get('voltage_nominal_v'),
                'ac_frequency_nominal_hz': ac.get('frequency_nominal_hz'),
                'power_factor_nominal': ac.get('power_factor_nominal'),
                'efficiency_dc_ac_nominal': eff.get('dc_ac_nominal'),
                'efficiency_dc_ac_min_acceptable': eff.get('dc_ac_min_acceptable'),
                'efficiency_system_min': eff_range[0] if len(eff_range) > 0 else 0.88,
                'efficiency_system_max': eff_range[1] if len(eff_range) > 1 else 0.92,
                'temp_coefficient_pct_per_c': thermal.get('temp_coefficient_power_pct_per_c'),
                'stc_temperature_c': thermal.get('stc_temperature_c'),
                'max_operating_temp_c': thermal.get('max_operating_temp_c'),
                'commissioned_date': commissioned
            }

            if dry_run:
                print(
                    f"[DRY-RUN] Would insert inverter: {inv_id} (Group {group_id})")
            else:
                with conn.cursor() as cur:
                    cur.execute(sql, params)

            inverter_count += 1

    print(f"Seeded {inverter_count} inverter assets")


def seed_poi_meter(conn, config: Dict[str, Any], dry_run: bool = False) -> None:
    """Seed POI meter asset"""
    plant_id = config.get('plant', {}).get('plant_id')
    poi = config.get('poi_meter_specs', {})
    power = poi.get('power_capacity', {})
    voltage = poi.get('grid_voltage', {})
    commissioned = config.get('plant', {}).get('commissioning_date')

    sql = """
        INSERT INTO silver.dim_asset (
            plant_id, asset_id, asset_type,
            nominal_capacity_kw,
            ac_voltage_nominal_v, ac_frequency_nominal_hz,
            max_export_kw, max_import_kw, meter_type,
            is_active, commissioned_date
        ) VALUES (
            %(plant_id)s::uuid, %(asset_id)s, 'poi_meter',
            %(nominal_capacity_kw)s,
            %(ac_voltage_nominal_v)s, %(ac_frequency_nominal_hz)s,
            %(max_export_kw)s, %(max_import_kw)s, %(meter_type)s,
            TRUE, %(commissioned_date)s
        )
        ON CONFLICT (plant_id, asset_id) DO UPDATE SET
            max_export_kw = EXCLUDED.max_export_kw,
            max_import_kw = EXCLUDED.max_import_kw,
            meter_type = EXCLUDED.meter_type,
            updated_at = CURRENT_TIMESTAMP
    """

    params = {
        'plant_id': plant_id,
        'asset_id': poi.get('meter_id', 'POI_METER_01'),
        'nominal_capacity_kw': power.get('max_export_kw'),
        'ac_voltage_nominal_v': voltage.get('nominal_v'),
        'ac_frequency_nominal_hz': 50,
        'max_export_kw': power.get('max_export_kw'),
        'max_import_kw': power.get('max_import_kw'),
        'meter_type': poi.get('meter_type'),
        'commissioned_date': commissioned
    }

    if dry_run:
        print(f"[DRY-RUN] Would insert POI meter: {params['asset_id']}")
        return

    with conn.cursor() as cur:
        cur.execute(sql, params)

    print(f"Seeded POI meter: {params['asset_id']}")


def seed_meteo_station(conn, config: Dict[str, Any], dry_run: bool = False) -> None:
    """Seed meteo station asset"""
    plant_id = config.get('plant', {}).get('plant_id')
    meteo = config.get('meteo_station_specs', {})
    commissioned = config.get('plant', {}).get('commissioning_date')

    sql = """
        INSERT INTO silver.dim_asset (
            plant_id, asset_id, asset_type,
            is_active, commissioned_date
        ) VALUES (
            %(plant_id)s::uuid, %(asset_id)s, 'meteo_station',
            TRUE, %(commissioned_date)s
        )
        ON CONFLICT (plant_id, asset_id) DO UPDATE SET
            is_active = EXCLUDED.is_active,
            updated_at = CURRENT_TIMESTAMP
    """

    params = {
        'plant_id': plant_id,
        'asset_id': meteo.get('station_id', 'METEO_01'),
        'commissioned_date': commissioned
    }

    if dry_run:
        print(f"[DRY-RUN] Would insert meteo station: {params['asset_id']}")
        return

    with conn.cursor() as cur:
        cur.execute(sql, params)

    print(f"Seeded meteo station: {params['asset_id']}")


def seed_thresholds(conn, config: Dict[str, Any], dry_run: bool = False) -> None:
    """Seed threshold configuration"""
    plant_id = config.get('plant', {}).get('plant_id')
    thresholds = config.get('thresholds', {})

    sql = """
        INSERT INTO silver.dim_thresholds (
            plant_id, threshold_category, threshold_name, 
            threshold_value, threshold_unit, severity, description
        ) VALUES (
            %(plant_id)s::uuid, %(category)s, %(name)s,
            %(value)s, %(unit)s, %(severity)s, %(description)s
        )
        ON CONFLICT (plant_id, threshold_category, threshold_name) DO UPDATE SET
            threshold_value = EXCLUDED.threshold_value,
            severity = EXCLUDED.severity,
            updated_at = CURRENT_TIMESTAMP
    """

    # Define threshold mappings with metadata
    threshold_definitions = {
        'inverter': {
            'efficiency_alert_min': {'unit': 'ratio', 'severity': 'alert', 'desc': 'Minimum DC-AC efficiency before alert'},
            'efficiency_warning_min': {'unit': 'ratio', 'severity': 'warning', 'desc': 'Minimum DC-AC efficiency before warning'},
            'temp_warning_c': {'unit': '°C', 'severity': 'warning', 'desc': 'Inverter temperature warning threshold'},
            'temp_critical_c': {'unit': '°C', 'severity': 'critical', 'desc': 'Inverter temperature critical threshold'},
            'freq_deviation_warning_hz': {'unit': 'Hz', 'severity': 'warning', 'desc': 'Grid frequency deviation warning'},
            'freq_deviation_critical_hz': {'unit': 'Hz', 'severity': 'critical', 'desc': 'Grid frequency deviation critical'},
        },
        'dc_diagnostics': {
            'string_current_imbalance_pct': {'unit': '%', 'severity': 'warning', 'desc': 'Maximum acceptable current imbalance between strings'},
            'voltage_deviation_pct': {'unit': '%', 'severity': 'warning', 'desc': 'Maximum acceptable DC voltage deviation from nominal'},
            'min_power_for_efficiency_calc_kw': {'unit': 'kW', 'severity': 'info', 'desc': 'Minimum DC power to calculate meaningful efficiency'},
        },
        'poi_meter': {
            'voltage_deviation_warning_pct': {'unit': '%', 'severity': 'warning', 'desc': 'Grid voltage deviation warning threshold'},
            'voltage_deviation_critical_pct': {'unit': '%', 'severity': 'critical', 'desc': 'Grid voltage deviation critical threshold'},
            'power_factor_warning_min': {'unit': 'ratio', 'severity': 'warning', 'desc': 'Minimum power factor before warning'},
            'power_factor_critical_min': {'unit': 'ratio', 'severity': 'critical', 'desc': 'Minimum power factor before critical alert'},
        }
    }

    count = 0
    for category, items in thresholds.items():
        definitions = threshold_definitions.get(category, {})
        for name, value in items.items():
            meta = definitions.get(
                name, {'unit': None, 'severity': 'info', 'desc': None})

            params = {
                'plant_id': plant_id,
                'category': category,
                'name': name,
                'value': value,
                'unit': meta['unit'],
                'severity': meta['severity'],
                'description': meta['desc']
            }

            if dry_run:
                print(
                    f"[DRY-RUN] Would insert threshold: {category}.{name} = {value}")
            else:
                with conn.cursor() as cur:
                    cur.execute(sql, params)

            count += 1

    print(f"Seeded {count} thresholds")


def main():
    parser = argparse.ArgumentParser(
        description='Seed PostgreSQL dimension tables from plant_config.json')
    parser.add_argument('--config', '-c', type=str,
                        help='Path to plant_config.json')
    parser.add_argument('--dry-run', '-n', action='store_true',
                        help='Print what would be done without executing')
    args = parser.parse_args()

    # Load configuration
    try:
        config_path = Path(args.config) if args.config else get_config_path()
        print(f"Loading configuration from: {config_path}")
        config = load_config(config_path)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        sys.exit(1)

    # Connect and seed
    if args.dry_run:
        print("\n=== DRY RUN MODE ===\n")
        seed_plant_config(None, config, dry_run=True)
        seed_inverter_assets(None, config, dry_run=True)
        seed_poi_meter(None, config, dry_run=True)
        seed_meteo_station(None, config, dry_run=True)
        seed_thresholds(None, config, dry_run=True)
        print("\n=== DRY RUN COMPLETE ===")
    else:
        try:
            conn = get_db_connection()
            print(f"Connected to PostgreSQL")

            seed_plant_config(conn, config)
            seed_inverter_assets(conn, config)
            seed_poi_meter(conn, config)
            seed_meteo_station(conn, config)
            seed_thresholds(conn, config)

            conn.commit()
            print("\nAll dimension tables seeded successfully!")
            conn.close()
        except Exception as e:
            print(f"Error: {e}")
            sys.exit(1)


if __name__ == "__main__":
    main()
