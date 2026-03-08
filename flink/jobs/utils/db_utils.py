"""
Database utilities for SolarX.ai medallion architecture
Handles connections and operations with PostgreSQL
"""

import psycopg2
from psycopg2 import Error
from psycopg2.extras import register_uuid
from contextlib import contextmanager
from typing import Dict, Optional, Any
import json
import logging
import uuid
import os
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Register UUID adapter for psycopg2
register_uuid()

# Load environment variables from .env file
load_dotenv()


def get_db_config() -> Dict[str, Any]:
    """Get database configuration from environment variables"""
    return {
        "host": os.getenv("DB_HOST"),
        "port": int(os.getenv("DB_PORT")),
        "database": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD")
    }


class DatabaseConfig:
    """Configuration for database connections"""
    
    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None
    ):
        """
        Initialize DatabaseConfig from environment variables or explicit parameters
        
        Args:
            host, port, database, user, password: If provided, override environment variables
        """
        if host is not None or port is not None:
            # Use explicitly provided parameters
            self.host = host
            self.port = port
            self.database = database
            self.user = user
            self.password = password
        else:
            # Load from environment variables
            config = get_db_config()
            self.host = config["host"]
            self.port = config["port"]
            self.database = config["database"]
            self.user = config["user"]
            self.password = config["password"]
    
    def get_connection_string(self) -> str:
        """Get the connection string for psycopg2"""
        return f"host={self.host} port={self.port} dbname={self.database} user={self.user} password={self.password}"


class DatabaseConnection:
    """Manages database connections with context manager support"""
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.connection = None
    
    def connect(self) -> None:
        """Establish database connection"""
        try:
            self.connection = psycopg2.connect(self.config.get_connection_string())
            logger.info(f"Connected to PostgreSQL at {self.config.host}:{self.config.port}")
        except Error as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def disconnect(self) -> None:
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("Disconnected from PostgreSQL")
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
    
    @contextmanager
    def cursor(self, commit: bool = False):
        """Context manager for database cursor"""
        cur = self.connection.cursor()
        try:
            yield cur
            if commit:
                self.connection.commit()
        except Error as e:
            self.connection.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            cur.close()


class BronzeLayerWriter:
    """Writes raw data to bronze layer tables"""
    
    def __init__(self, db_config: DatabaseConfig):
        self.db_config = db_config
    
    def insert_inverter_data(self, data: Dict[str, Any]) -> Optional[int]:
        """
        Insert inverter data into bronze layer
        
        Args:
            data: Dictionary with keys: inverter_id, timestamp, state, inverter_temp_C, 
                  ac_power_kW, ac_freq_Hz, active_failures, failure_types
        
        Returns:
            Database row ID if successful, None otherwise
        """
        try:
            with DatabaseConnection(self.db_config) as db:
                with db.cursor(commit=True) as cur:
                    cur.execute(
                        """
                        INSERT INTO bronze.inverter_raw 
                        (plant_id, inverter_id, timestamp, state, inverter_temp_C, ac_power_kW, 
                         ac_freq_Hz, dc_power_kW, dc_voltage_V, dc_current_A, healthy_strings, failed_strings,
                         active_failures, failure_types, raw_json)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                        """,
                        (
                            data.get('guid'),
                            data.get('inverter_id'),
                            data.get('timestamp'),
                            int(data.get('state', 0)) if data.get('state') is not None else None,
                            float(data.get('inverter_temp_C', 0)) if data.get('inverter_temp_C') is not None else None,
                            float(data.get('ac_power_kW', 0)) if data.get('ac_power_kW') is not None else None,
                            float(data.get('ac_freq_Hz', 0)) if data.get('ac_freq_Hz') is not None else None,
                            float(data.get('dc_power_kW', 0)) if data.get('dc_power_kW') is not None else None,
                            float(data.get('dc_voltage_V', 0)) if data.get('dc_voltage_V') is not None else None,
                            float(data.get('dc_current_A', 0)) if data.get('dc_current_A') is not None else None,
                            int(data.get('healthy_strings', 0)) if data.get('healthy_strings') is not None else None,
                            int(data.get('failed_strings', 0)) if data.get('failed_strings') is not None else None,
                            int(data.get('active_failures', 0)) if data.get('active_failures') is not None else None,
                            json.dumps(data.get('failure_types', [])),
                            json.dumps(data)
                        )
                    )
                    row_id = cur.fetchone()[0]
                    logger.info(f"Inserted inverter data with ID {row_id}")
                    return row_id
        except Error as e:
            logger.error(f"Failed to insert inverter data: {e}")
            raise e
    
    def insert_poi_meter_data(self, data: Dict[str, Any]) -> Optional[int]:
        """
        Insert POI meter data into bronze layer
        
        Args:
            data: Dictionary with keys: guid, timestamp, export_active_power_kW, import_active_power_kW,
                  reactive_power_kVAr, grid_voltage_l1_V, grid_voltage_l2_V, grid_voltage_l3_V,
                  grid_frequency_Hz, power_factor, active_failures, connection_issues
        
        Returns:
            Database row ID if successful, None otherwise
        """
        try:
            with DatabaseConnection(self.db_config) as db:
                with db.cursor(commit=True) as cur:
                    cur.execute(
                        """
                        INSERT INTO bronze.poi_meter_raw 
                        (plant_id, timestamp, export_active_power_kW, import_active_power_kW,
                         reactive_power_kVAr, grid_voltage_l1_V, grid_voltage_l2_V, grid_voltage_l3_V,
                         grid_frequency_Hz, power_factor, active_failures, connection_issues, raw_json)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                        """,
                        (
                            data.get('guid'),
                            data.get('timestamp'),
                            float(data.get('export_active_power_kW', 0)) if data.get('export_active_power_kW') is not None else None,
                            float(data.get('import_active_power_kW', 0)) if data.get('import_active_power_kW') is not None else None,
                            float(data.get('reactive_power_kVAr', 0)) if data.get('reactive_power_kVAr') is not None else None,
                            float(data.get('grid_voltage_l1_V', 0)) if data.get('grid_voltage_l1_V') is not None else None,
                            float(data.get('grid_voltage_l2_V', 0)) if data.get('grid_voltage_l2_V') is not None else None,
                            float(data.get('grid_voltage_l3_V', 0)) if data.get('grid_voltage_l3_V') is not None else None,
                            float(data.get('grid_frequency_Hz', 0)) if data.get('grid_frequency_Hz') is not None else None,
                            float(data.get('power_factor', 0)) if data.get('power_factor') is not None else None,
                            int(data.get('active_failures', 0)) if data.get('active_failures') is not None else None,
                            bool(data.get('connection_issues', False)),
                            json.dumps(data)
                        )
                    )
                    row_id = cur.fetchone()[0]
                    logger.info(f"Inserted POI meter data with ID {row_id}")
                    return row_id
        except Error as e:
            logger.error(f"Failed to insert POI meter data: {e}")
            raise e
    
    def insert_meteo_data(self, data: Dict[str, Any]) -> Optional[int]:
        """
        Insert meteorological station data into bronze layer
        
        Args:
            data: Dictionary with keys: timestamp, amb_temp_C, module_temp_C, wind_speed_ms,
                  wind_dir_deg, humidity_percent, poa_irradiance_wm2,
                  active_failures, sensor_malfunctions
        
        Returns:
            Database row ID if successful, None otherwise
        """
        try:
            with DatabaseConnection(self.db_config) as db:
                with db.cursor(commit=True) as cur:
                    cur.execute(
                        """
                        INSERT INTO bronze.meteo_station_raw 
                        (plant_id, timestamp, amb_temp_C, module_temp_C, wind_speed_ms, 
                         wind_dir_deg, humidity_percent, poa_irradiance_wm2, raw_json)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                        """,
                        (
                            data.get('guid'),
                            data.get('timestamp'),
                            float(data.get('amb_temp_C', 0)) if data.get('amb_temp_C') is not None else None,
                            float(data.get('module_temp_C', 0)) if data.get('module_temp_C') is not None else None,
                            float(data.get('wind_speed_ms', 0)) if data.get('wind_speed_ms') is not None else None,
                            float(data.get('wind_dir_deg', 0)) if data.get('wind_dir_deg') is not None else None,
                            float(data.get('humidity_percent', 0)) if data.get('humidity_percent') is not None else None,
                            float(data.get('poa_irradiance_wm2', 0)) if data.get('poa_irradiance_wm2') is not None else None,
                            json.dumps(data)
                        )
                    )
                    row_id = cur.fetchone()[0]
                    logger.info(f"Inserted meteo data with ID {row_id}")
                    return row_id
        except Error as e:
            logger.error(f"Failed to insert meteo data: {e}")
            raise e
    
    def insert_system_status_data(self, data: Dict[str, Any]) -> Optional[int]:
        """
        Insert system status data into bronze layer
        
        Args:
            data: Dictionary with nested structure:
                  - timestamp
                  - system_impact: {total_failures, stress_level, affected_components, critical_failures}
                  - component_status: {total, healthy, failed}
        
        Returns:
            Database row ID if successful, None otherwise
        """
        # Extract nested data
        system_impact = data.get('system_impact', {})
        component_status = data.get('component_status', {})
        
        # affected_components is a list, so we need to serialize it
        affected_components = system_impact.get('affected_components', [])
        if isinstance(affected_components, list):
            affected_components_json = json.dumps(affected_components)
            affected_components_count = len(affected_components)
        else:
            affected_components_json = json.dumps([])
            affected_components_count = 0
        
        try:
            with DatabaseConnection(self.db_config) as db:
                with db.cursor(commit=True) as cur:
                    cur.execute(
                        """
                        INSERT INTO bronze.system_status_raw 
                        (plant_id, timestamp, total_failures, stress_level,
                         affected_components, critical_failures, total_components, 
                         healthy_components, failed_components, raw_json)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                        """,
                        (
                            data.get('guid'),
                            data.get('timestamp'),
                            int(system_impact.get('total_failures', 0)) if system_impact.get('total_failures') is not None else None,
                            float(system_impact.get('stress_level', 0)) if system_impact.get('stress_level') is not None else None,
                            affected_components_count,
                            int(system_impact.get('critical_failures', 0)) if system_impact.get('critical_failures') is not None else None,
                            int(component_status.get('total', 0)) if component_status.get('total') is not None else None,
                            int(component_status.get('healthy', 0)) if component_status.get('healthy') is not None else None,
                            int(component_status.get('failed', 0)) if component_status.get('failed') is not None else None,
                            json.dumps(data)
                        )
                    )
                    row_id = cur.fetchone()[0]
                    logger.info(f"Inserted system status data with ID {row_id}")
                    return row_id
        except Error as e:
            logger.error(f"Failed to insert system status data: {e}")
            raise e


class SilverLayerWriter:
    """
    Processes and writes cleaned data from Bronze to Silver layer.
    Handles data quality checks, validation, and transformation.
    """
    
    def __init__(self, db_config: DatabaseConfig):
        self.db_config = db_config
    
    def get_unprocessed_inverter_records(self, batch_size: int = 100) -> list:
        """Fetch and mark unprocessed inverter records as 'processing' atomically."""
        try:
            with DatabaseConnection(self.db_config) as db:
                with db.cursor(commit=True) as cur:
                    cur.execute(
                        """
                        SELECT id FROM bronze.inverter_raw
                        WHERE processing_status = 'new'
                        ORDER BY ingestion_timestamp ASC
                        LIMIT %s
                        FOR UPDATE SKIP LOCKED
                        """,
                        (batch_size,)
                    )
                    ids = [row[0] for row in cur.fetchall()]
                    if not ids:
                        return []
                    # Mark as processing
                    cur.execute(
                        f"""
                        UPDATE bronze.inverter_raw
                        SET processing_status = 'processing'
                        WHERE id = ANY(%s)
                        """,
                        (ids,)
                    )
                    # Fetch full records
                    cur.execute(
                        f"""
                        SELECT id, plant_id, inverter_id, timestamp, state, inverter_temp_C, 
                               ac_power_kW, ac_freq_Hz, dc_power_kW, dc_voltage_V, dc_current_A,
                               healthy_strings, failed_strings, active_failures, failure_types
                        FROM bronze.inverter_raw
                        WHERE id = ANY(%s)
                        """,
                        (ids,)
                    )
                    columns = ['id', 'plant_id', 'inverter_id', 'timestamp', 'state', 
                              'inverter_temp_C', 'ac_power_kW', 'ac_freq_Hz', 
                              'dc_power_kW', 'dc_voltage_V', 'dc_current_A',
                              'healthy_strings', 'failed_strings',
                              'active_failures', 'failure_types']
                    return [dict(zip(columns, row)) for row in cur.fetchall()]
        except Error as e:
            logger.error(f"Failed to fetch/mark unprocessed inverter records: {e}")
            return []
    
    def get_unprocessed_poi_meter_records(self, batch_size: int = 100) -> list:
        """Fetch and mark unprocessed POI meter records as 'processing' atomically."""
        try:
            with DatabaseConnection(self.db_config) as db:
                with db.cursor(commit=True) as cur:
                    cur.execute(
                        """
                        SELECT id FROM bronze.poi_meter_raw
                        WHERE processing_status = 'new'
                        ORDER BY ingestion_timestamp ASC
                        LIMIT %s
                        FOR UPDATE SKIP LOCKED
                        """,
                        (batch_size,)
                    )
                    ids = [row[0] for row in cur.fetchall()]
                    if not ids:
                        return []
                    cur.execute(
                        f"""
                        UPDATE bronze.poi_meter_raw
                        SET processing_status = 'processing'
                        WHERE id = ANY(%s)
                        """,
                        (ids,)
                    )
                    cur.execute(
                        f"""
                        SELECT id, plant_id, timestamp, export_active_power_kW, import_active_power_kW,
                               reactive_power_kVAr, grid_voltage_l1_V, grid_voltage_l2_V, grid_voltage_l3_V,
                               grid_frequency_Hz, power_factor, active_failures, connection_issues
                        FROM bronze.poi_meter_raw
                        WHERE id = ANY(%s)
                        """,
                        (ids,)
                    )
                    columns = ['id', 'plant_id', 'timestamp', 'export_active_power_kW', 'import_active_power_kW',
                              'reactive_power_kVAr', 'grid_voltage_l1_V', 'grid_voltage_l2_V', 'grid_voltage_l3_V',
                              'grid_frequency_Hz', 'power_factor', 'active_failures', 'connection_issues']
                    return [dict(zip(columns, row)) for row in cur.fetchall()]
        except Error as e:
            logger.error(f"Failed to fetch/mark unprocessed POI meter records: {e}")
            return []
    
    def get_unprocessed_meteo_records(self, batch_size: int = 100) -> list:
        """Fetch and mark unprocessed meteo records as 'processing' atomically."""
        try:
            with DatabaseConnection(self.db_config) as db:
                with db.cursor(commit=True) as cur:
                    cur.execute(
                        """
                        SELECT id FROM bronze.meteo_station_raw
                        WHERE processing_status = 'new'
                        ORDER BY ingestion_timestamp ASC
                        LIMIT %s
                        FOR UPDATE SKIP LOCKED
                        """,
                        (batch_size,)
                    )
                    ids = [row[0] for row in cur.fetchall()]
                    if not ids:
                        return []
                    cur.execute(
                        f"""
                        UPDATE bronze.meteo_station_raw
                        SET processing_status = 'processing'
                        WHERE id = ANY(%s)
                        """,
                        (ids,)
                    )
                    cur.execute(
                        f"""
                        SELECT id, plant_id, timestamp, amb_temp_C, module_temp_C, 
                               wind_speed_ms, wind_dir_deg, humidity_percent, 
                               poa_irradiance_wm2
                        FROM bronze.meteo_station_raw
                        WHERE id = ANY(%s)
                        """,
                        (ids,)
                    )
                    columns = ['id', 'plant_id', 'timestamp', 'amb_temp_C', 'module_temp_C',
                              'wind_speed_ms', 'wind_dir_deg', 'humidity_percent',
                              'poa_irradiance_wm2']
                    return [dict(zip(columns, row)) for row in cur.fetchall()]
        except Error as e:
            logger.error(f"Failed to fetch/mark unprocessed meteo records: {e}")
            return []
    
    def get_unprocessed_system_status_records(self, batch_size: int = 100) -> list:
        """Fetch and mark unprocessed system status records as 'processing' atomically."""
        try:
            with DatabaseConnection(self.db_config) as db:
                with db.cursor(commit=True) as cur:
                    cur.execute(
                        """
                        SELECT id FROM bronze.system_status_raw
                        WHERE processing_status = 'new'
                        ORDER BY ingestion_timestamp ASC
                        LIMIT %s
                        FOR UPDATE SKIP LOCKED
                        """,
                        (batch_size,)
                    )
                    ids = [row[0] for row in cur.fetchall()]
                    if not ids:
                        return []
                    cur.execute(
                        f"""
                        UPDATE bronze.system_status_raw
                        SET processing_status = 'processing'
                        WHERE id = ANY(%s)
                        """,
                        (ids,)
                    )
                    cur.execute(
                        f"""
                        SELECT id, plant_id, timestamp, total_failures, stress_level,
                               affected_components, critical_failures, total_components,
                               healthy_components, failed_components
                        FROM bronze.system_status_raw
                        WHERE id = ANY(%s)
                        """,
                        (ids,)
                    )
                    columns = ['id', 'plant_id', 'timestamp', 'total_failures', 'stress_level',
                              'affected_components', 'critical_failures', 'total_components',
                              'healthy_components', 'failed_components']
                    return [dict(zip(columns, row)) for row in cur.fetchall()]
        except Error as e:
            logger.error(f"Failed to fetch/mark unprocessed system status records: {e}")
            return []
    
    def _validate_inverter_data(self, data: Dict[str, Any]) -> tuple:
        """
        Validate inverter data and determine quality flag.
        Returns (data_quality_flag, anomaly_flag)
        """
        anomaly_flag = False
        quality_issues = []
        
        # Check for null values in critical fields
        if data.get('ac_power_kW') is None:
            quality_issues.append('missing_power')
        if data.get('inverter_temp_C') is None:
            quality_issues.append('missing_temp')
        if data.get('timestamp') is None:
            quality_issues.append('missing_timestamp')
        
        # Check for out-of-range values
        if data.get('ac_power_kW') is not None:
            power = float(data['ac_power_kW'])
            if power < 0:
                quality_issues.append('negative_power')
                anomaly_flag = True
            elif power > 1000:  # Assuming max 1MW per inverter
                quality_issues.append('excessive_power')
                anomaly_flag = True
        
        # DC side validation
        if data.get('dc_power_kW') is not None:
            dc_power = float(data['dc_power_kW'])
            if dc_power < 0:
                quality_issues.append('negative_dc_power')
                anomaly_flag = True
        
        if data.get('dc_voltage_V') is not None:
            dc_voltage = float(data['dc_voltage_V'])
            if dc_voltage < 0 or dc_voltage > 1500:  # Typical DC voltage range
                quality_issues.append('dc_voltage_out_of_range')
                anomaly_flag = True
        
        if data.get('dc_current_A') is not None:
            dc_current = float(data['dc_current_A'])
            if dc_current < 0:
                quality_issues.append('negative_dc_current')
                anomaly_flag = True
        
        # String health validation
        if data.get('failed_strings') is not None and data.get('failed_strings') > 0:
            anomaly_flag = True
        
        if data.get('inverter_temp_C') is not None:
            temp = float(data['inverter_temp_C'])
            if temp < -40 or temp > 100:
                quality_issues.append('temp_out_of_range')
                anomaly_flag = True
        
        if data.get('ac_freq_Hz') is not None:
            power = float(data['ac_power_kW'])
            freq = float(data['ac_freq_Hz'])
            if (freq < 45 or freq > 65) and (power > 0):  # Normal range for grid frequency
                quality_issues.append('freq_out_of_range')
                anomaly_flag = True
        
        # Check for failures
        if data.get('active_failures', 0) > 0:
            anomaly_flag = True
        
        quality_flag = 'valid' if not quality_issues else ','.join(quality_issues)
        return quality_flag, anomaly_flag
    
    def _validate_poi_meter_data(self, data: Dict[str, Any]) -> tuple:
        """Validate POI meter data and determine quality flag."""
        anomaly_flag = False
        quality_issues = []
        
        # Check export/import exclusivity
        export_power = data.get('export_active_power_kW', 0)
        import_power = data.get('import_active_power_kW', 0)
        
        if export_power is not None and float(export_power) < 0:
            quality_issues.append('negative_export')
            anomaly_flag = True
        
        if import_power is not None and float(import_power) < 0:
            quality_issues.append('negative_import')
            anomaly_flag = True
        
        # Both should not be positive simultaneously
        if export_power and import_power and float(export_power) > 0 and float(import_power) > 0:
            quality_issues.append('simultaneous_export_import')
            anomaly_flag = True
        
        # Grid voltage validation (3-phase)
        for phase in ['l1', 'l2', 'l3']:
            voltage_key = f'grid_voltage_{phase}_V'
            if data.get(voltage_key) is not None:
                voltage = float(data[voltage_key])
                if voltage < 300 or voltage > 500:  # Typical range for 400V nominal
                    quality_issues.append(f'{phase}_voltage_out_of_range')
                    anomaly_flag = True
        
        # Grid frequency validation
        if data.get('grid_frequency_Hz') is not None:
            freq = float(data['grid_frequency_Hz'])
            if freq < 49 or freq > 51:  # 50Hz ±1Hz
                quality_issues.append('frequency_deviation')
                anomaly_flag = True
        
        # Power factor validation
        if data.get('power_factor') is not None:
            pf = float(data['power_factor'])
            if pf < 0 or pf > 1:
                quality_issues.append('pf_out_of_range')
                anomaly_flag = True
        
        if data.get('connection_issues'):
            anomaly_flag = True
        
        if data.get('active_failures', 0) > 0:
            anomaly_flag = True
        
        quality_flag = 'valid' if not quality_issues else ','.join(quality_issues)
        return quality_flag, anomaly_flag
    
    def _validate_meteo_data(self, data: Dict[str, Any]) -> tuple:
        """Validate meteo data and determine quality flag."""
        anomaly_flag = False
        quality_issues = []
        
        if data.get('amb_temp_C') is not None:
            temp = float(data['amb_temp_C'])
            if temp < -50 or temp > 60:
                quality_issues.append('amb_temp_out_of_range')
                anomaly_flag = True
        
        if data.get('module_temp_C') is not None:
            temp = float(data['module_temp_C'])
            if temp < -50 or temp > 100:
                quality_issues.append('module_temp_out_of_range')
                anomaly_flag = True
        
        if data.get('humidity_percent') is not None:
            hum = float(data['humidity_percent'])
            if hum < 0 or hum > 100:
                quality_issues.append('humidity_out_of_range')
                anomaly_flag = True
        
        if data.get('poa_irradiance_wm2') is not None:
            irr = float(data['poa_irradiance_wm2'])
            if irr < 0 or irr > 1500:
                quality_issues.append('irradiance_out_of_range')
                anomaly_flag = True
        
        if data.get('wind_speed_ms') is not None:
            wind = float(data['wind_speed_ms'])
            if wind < 0 or wind > 100:
                quality_issues.append('wind_out_of_range')
                anomaly_flag = True
        
        quality_flag = 'valid' if not quality_issues else ','.join(quality_issues)
        return quality_flag, anomaly_flag
    
    def _validate_system_status_data(self, data: Dict[str, Any]) -> tuple:
        """Validate system status data and determine quality flag."""
        anomaly_flag = False
        quality_issues = []
        
        if data.get('stress_level') is not None:
            stress = float(data['stress_level'])
            if stress < 0 or stress > 1:
                quality_issues.append('stress_out_of_range')
            if stress > 0.7:
                quality_issues.append('high_stress')
                anomaly_flag = True
        
        if data.get('total_failures', 0) > 0:
            quality_issues.append('failures_present')
            anomaly_flag = True
        
        if data.get('critical_failures', 0) > 0:
            quality_issues.append('critical_failures_present')
            anomaly_flag = True
        
        quality_flag = 'valid' if not quality_issues else ','.join(quality_issues)
        return quality_flag, anomaly_flag
    
    def insert_silver_inverter(self, data: Dict[str, Any]) -> Optional[int]:
        """Insert validated inverter data into silver layer"""
        quality_flag, anomaly_flag = self._validate_inverter_data(data)
        
        # Calculate instant efficiency if DC power is available
        instant_efficiency = None
        efficiency_flag = 'normal'
        if data.get('dc_power_kW') and data.get('ac_power_kW'):
            dc_power = float(data['dc_power_kW'])
            ac_power = float(data['ac_power_kW'])
            if dc_power > 0:
                instant_efficiency = ac_power / dc_power
                if instant_efficiency < 0.93:
                    efficiency_flag = 'alert'
                elif instant_efficiency < 0.95:
                    efficiency_flag = 'warning'
        
        try:
            with DatabaseConnection(self.db_config) as db:
                with db.cursor(commit=True) as cur:
                    # Insert into silver layer
                    cur.execute(
                        """
                        INSERT INTO silver.inverter_cleaned 
                        (plant_id, inverter_id, timestamp, state, inverter_temp_C, 
                         ac_power_kW, ac_freq_Hz, dc_power_kW, dc_voltage_V, dc_current_A,
                         instant_efficiency, efficiency_flag, healthy_strings, failed_strings,
                         active_failures, failure_types,
                         source_bronze_id, data_quality_flag, anomaly_flag)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (plant_id, inverter_id, timestamp) DO UPDATE SET
                            state = EXCLUDED.state,
                            inverter_temp_C = EXCLUDED.inverter_temp_C,
                            ac_power_kW = EXCLUDED.ac_power_kW,
                            ac_freq_Hz = EXCLUDED.ac_freq_Hz,
                            dc_power_kW = EXCLUDED.dc_power_kW,
                            dc_voltage_V = EXCLUDED.dc_voltage_V,
                            dc_current_A = EXCLUDED.dc_current_A,
                            instant_efficiency = EXCLUDED.instant_efficiency,
                            efficiency_flag = EXCLUDED.efficiency_flag,
                            healthy_strings = EXCLUDED.healthy_strings,
                            failed_strings = EXCLUDED.failed_strings,
                            active_failures = EXCLUDED.active_failures,
                            failure_types = EXCLUDED.failure_types,
                            source_bronze_id = EXCLUDED.source_bronze_id,
                            data_quality_flag = EXCLUDED.data_quality_flag,
                            anomaly_flag = EXCLUDED.anomaly_flag,
                            processed_timestamp = CURRENT_TIMESTAMP
                        RETURNING id
                        """,
                        (
                            data.get('plant_id'),
                            data.get('inverter_id'),
                            data.get('timestamp'),
                            data.get('state'),
                            data.get('inverter_temp_C'),
                            data.get('ac_power_kW'),
                            data.get('ac_freq_Hz'),
                            data.get('dc_power_kW'),
                            data.get('dc_voltage_V'),
                            data.get('dc_current_A'),
                            instant_efficiency,
                            efficiency_flag,
                            data.get('healthy_strings'),
                            data.get('failed_strings'),
                            data.get('active_failures'),
                            json.dumps(data.get('failure_types', [])) if isinstance(data.get('failure_types'), (list, dict)) else data.get('failure_types'),
                            data.get('id'),  # source_bronze_id
                            quality_flag,
                            anomaly_flag
                        )
                    )
                    row_id = cur.fetchone()[0]
                    
                    # Update bronze layer processing status
                    cur.execute(
                        """
                        UPDATE bronze.inverter_raw 
                        SET processing_status = 'processed'
                        WHERE id = %s
                        """,
                        (data.get('id'),)
                    )
                    
                    return row_id
        except Error as e:
            logger.error(f"Failed to insert silver inverter data: {e}")
            # Mark bronze record as failed
            try:
                with DatabaseConnection(self.db_config) as db:
                    with db.cursor(commit=True) as cur:
                        cur.execute(
                            """
                            UPDATE bronze.inverter_raw 
                            SET processing_status = 'failed', error_message = %s
                            WHERE id = %s
                            """,
                            (str(e), data.get('id'))
                        )
            except Error:
                pass
            raise e
    
    def insert_silver_poi_meter(self, data: Dict[str, Any]) -> Optional[int]:
        """Insert validated POI meter data into silver layer"""
        quality_flag, anomaly_flag = self._validate_poi_meter_data(data)
        
        try:
            with DatabaseConnection(self.db_config) as db:
                with db.cursor(commit=True) as cur:
                    cur.execute(
                        """
                        INSERT INTO silver.poi_meter_cleaned 
                        (plant_id, timestamp, export_active_power_kW, import_active_power_kW,
                         reactive_power_kVAr, grid_voltage_l1_V, grid_voltage_l2_V, grid_voltage_l3_V,
                         grid_frequency_Hz, power_factor, active_failures, connection_issues,
                         source_bronze_id, data_quality_flag, anomaly_flag)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (plant_id, timestamp) DO UPDATE SET
                            export_active_power_kW = EXCLUDED.export_active_power_kW,
                            import_active_power_kW = EXCLUDED.import_active_power_kW,
                            reactive_power_kVAr = EXCLUDED.reactive_power_kVAr,
                            grid_voltage_l1_V = EXCLUDED.grid_voltage_l1_V,
                            grid_voltage_l2_V = EXCLUDED.grid_voltage_l2_V,
                            grid_voltage_l3_V = EXCLUDED.grid_voltage_l3_V,
                            grid_frequency_Hz = EXCLUDED.grid_frequency_Hz,
                            power_factor = EXCLUDED.power_factor,
                            active_failures = EXCLUDED.active_failures,
                            connection_issues = EXCLUDED.connection_issues,
                            source_bronze_id = EXCLUDED.source_bronze_id,
                            data_quality_flag = EXCLUDED.data_quality_flag,
                            anomaly_flag = EXCLUDED.anomaly_flag,
                            processed_timestamp = CURRENT_TIMESTAMP
                        RETURNING id
                        """,
                        (
                            data.get('plant_id'),
                            data.get('timestamp'),
                            data.get('export_active_power_kW'),
                            data.get('import_active_power_kW'),
                            data.get('reactive_power_kVAr'),
                            data.get('grid_voltage_l1_V'),
                            data.get('grid_voltage_l2_V'),
                            data.get('grid_voltage_l3_V'),
                            data.get('grid_frequency_Hz'),
                            data.get('power_factor'),
                            data.get('active_failures'),
                            data.get('connection_issues'),
                            data.get('id'),  # source_bronze_id
                            quality_flag,
                            anomaly_flag
                        )
                    )
                    row_id = cur.fetchone()[0]
                    
                    cur.execute(
                        """
                        UPDATE bronze.poi_meter_raw 
                        SET processing_status = 'processed'
                        WHERE id = %s
                        """,
                        (data.get('id'),)
                    )
                    
                    return row_id
        except Error as e:
            logger.error(f"Failed to insert silver POI meter data: {e}")
            try:
                with DatabaseConnection(self.db_config) as db:
                    with db.cursor(commit=True) as cur:
                        cur.execute(
                            """
                            UPDATE bronze.poi_meter_raw 
                            SET processing_status = 'failed', error_message = %s
                            WHERE id = %s
                            """,
                            (str(e), data.get('id'))
                        )
            except Error:
                pass
            raise e
    
    def insert_silver_meteo(self, data: Dict[str, Any]) -> Optional[int]:
        """Insert validated meteo data into silver layer"""
        quality_flag, anomaly_flag = self._validate_meteo_data(data)
        
        try:
            with DatabaseConnection(self.db_config) as db:
                with db.cursor(commit=True) as cur:
                    cur.execute(
                        """
                        INSERT INTO silver.meteo_station_cleaned 
                        (plant_id, timestamp, amb_temp_C, module_temp_C, wind_speed_ms,
                         wind_dir_deg, humidity_percent, poa_irradiance_wm2,
                         source_bronze_id, data_quality_flag, anomaly_flag)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (plant_id, timestamp) DO UPDATE SET
                            timestamp = EXCLUDED.timestamp,
                            amb_temp_C = EXCLUDED.amb_temp_C,
                            module_temp_C = EXCLUDED.module_temp_C,
                            wind_speed_ms = EXCLUDED.wind_speed_ms,
                            wind_dir_deg = EXCLUDED.wind_dir_deg,
                            humidity_percent = EXCLUDED.humidity_percent,
                            poa_irradiance_wm2 = EXCLUDED.poa_irradiance_wm2,
                            source_bronze_id = EXCLUDED.source_bronze_id,
                            data_quality_flag = EXCLUDED.data_quality_flag,
                            anomaly_flag = EXCLUDED.anomaly_flag,
                            processed_timestamp = CURRENT_TIMESTAMP
                        RETURNING id
                        """,
                        (
                            data.get('plant_id'),
                            data.get('timestamp'),
                            data.get('amb_temp_C'),
                            data.get('module_temp_C'),
                            data.get('wind_speed_ms'),
                            data.get('wind_dir_deg'),
                            data.get('humidity_percent'),
                            data.get('poa_irradiance_wm2'),
                            data.get('id'),
                            quality_flag,
                            anomaly_flag
                        )
                    )
                    row_id = cur.fetchone()[0]
                    
                    cur.execute(
                        """
                        UPDATE bronze.meteo_station_raw 
                        SET processing_status = 'processed'
                        WHERE id = %s
                        """,
                        (data.get('id'),)
                    )
                    
                    return row_id
        except Error as e:
            logger.error(f"Failed to insert silver meteo data: {e}")
            try:
                with DatabaseConnection(self.db_config) as db:
                    with db.cursor(commit=True) as cur:
                        cur.execute(
                            """
                            UPDATE bronze.meteo_station_raw 
                            SET processing_status = 'failed', error_message = %s
                            WHERE id = %s
                            """,
                            (str(e), data.get('id'))
                        )
            except Error:
                pass
            raise e
    
    def insert_silver_system_status(self, data: Dict[str, Any]) -> Optional[int]:
        """Insert validated system status data into silver layer"""
        quality_flag, anomaly_flag = self._validate_system_status_data(data)
        
        try:
            with DatabaseConnection(self.db_config) as db:
                with db.cursor(commit=True) as cur:
                    cur.execute(
                        """
                        INSERT INTO silver.system_status_cleaned 
                        (plant_id, timestamp, total_failures, stress_level,
                         affected_components, critical_failures, total_components,
                         healthy_components, failed_components, source_bronze_id,
                         data_quality_flag, anomaly_flag)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (plant_id, timestamp) DO UPDATE SET
                            timestamp = EXCLUDED.timestamp,
                            total_failures = EXCLUDED.total_failures,
                            stress_level = EXCLUDED.stress_level,
                            affected_components = EXCLUDED.affected_components,
                            critical_failures = EXCLUDED.critical_failures,
                            total_components = EXCLUDED.total_components,
                            healthy_components = EXCLUDED.healthy_components,
                            failed_components = EXCLUDED.failed_components,
                            source_bronze_id = EXCLUDED.source_bronze_id,
                            data_quality_flag = EXCLUDED.data_quality_flag,
                            anomaly_flag = EXCLUDED.anomaly_flag,
                            processed_timestamp = CURRENT_TIMESTAMP
                        RETURNING id
                        """,
                        (
                            data.get('plant_id'),
                            data.get('timestamp'),
                            data.get('total_failures'),
                            data.get('stress_level'),
                            data.get('affected_components'),
                            data.get('critical_failures'),
                            data.get('total_components'),
                            data.get('healthy_components'),
                            data.get('failed_components'),
                            data.get('id'),
                            quality_flag,
                            anomaly_flag
                        )
                    )
                    row_id = cur.fetchone()[0]
                    
                    cur.execute(
                        """
                        UPDATE bronze.system_status_raw 
                        SET processing_status = 'processed'
                        WHERE id = %s
                        """,
                        (data.get('id'),)
                    )
                    
                    return row_id
        except Error as e:
            logger.error(f"Failed to insert silver system status data: {e}")
            try:
                with DatabaseConnection(self.db_config) as db:
                    with db.cursor(commit=True) as cur:
                        cur.execute(
                            """
                            UPDATE bronze.system_status_raw 
                            SET processing_status = 'failed', error_message = %s
                            WHERE id = %s
                            """,
                            (str(e), data.get('id'))
                        )
            except Error:
                pass
            raise e

class GoldLayerWriter:
    """
    Processes and writes aggregated data from Silver to Gold layer.
    Handles hourly and daily aggregations for all components.
    """
    
    # =========================================================================
    # PLANT CAPACITY CONFIGURATION (extracted from simulator)
    # =========================================================================
    # Inverter configuration:
    #   - Group A: 4 inverters (A0, A1, A2, A3)
    #   - Group B: 4 inverters (B0, B1, B2, B3)
    #   - Group C: 6 inverters (C0, C1, C2, C3, C4, C5)
    #   - Total: 14 inverters
    # Per inverter:
    #   - Surface: 100 m²
    #   - Efficiency: ~90% (0.88-0.92)
    #   - Peak power at STC (1000 W/m²): 90 kWp
    # Total plant capacity: 14 × 90 kWp = 1,260 kWp
    # =========================================================================
    
    INVERTER_CAPACITY_KWP = 90.0          # Peak power per inverter (kWp)
    TOTAL_INVERTERS = 14                   # Total number of inverters
    PLANT_CAPACITY_KWP = 1260.0           # Total plant capacity (kWp)
    PANEL_SURFACE_M2 = 100.0              # Panel surface per inverter (m²)
    TOTAL_SURFACE_M2 = 1400.0             # Total panel surface (m²)
    MODULE_TEMP_COEFFICIENT = -0.004      # Typical: -0.4%/°C power loss
    STC_TEMPERATURE = 25.0                # Standard Test Condition temp (°C)
    
    def __init__(self, db_config: DatabaseConfig):
        self.db_config = db_config
    
    # =========================================================================
    # UNAGGREGATED DATA DISCOVERY (for data-driven aggregation)
    # =========================================================================
    
    def get_unaggregated_inverter_hours(self) -> list:
        """
        Get distinct (plant_id, inverter_id, hour) from silver that aren't in gold yet.
        """
        try:
            with DatabaseConnection(self.db_config) as db:
                with db.cursor() as cur:
                    cur.execute("""
                        SELECT DISTINCT s.plant_id, s.inverter_id, date_trunc('hour', s.timestamp) as hour
                        FROM silver.inverter_cleaned s
                        LEFT JOIN gold.inverter_hourly g 
                            ON s.plant_id = g.plant_id 
                            AND s.inverter_id = g.inverter_id
                            AND date_trunc('hour', s.timestamp) = g.timestamp_hour
                        WHERE g.id IS NULL
                        ORDER BY hour
                        LIMIT 200
                    """)
                    return cur.fetchall()
        except Exception as e:
            logger.error(f"Error getting unaggregated inverter hours: {e}")
            return []
    
    def get_unaggregated_inverter_dates(self) -> list:
        """
        Get distinct (plant_id, inverter_id, date) from silver hourly that aren't in gold daily yet.
        """
        try:
            with DatabaseConnection(self.db_config) as db:
                with db.cursor() as cur:
                    cur.execute("""
                        SELECT DISTINCT s.plant_id, s.inverter_id, date_trunc('day', s.timestamp_hour)::date as day
                        FROM gold.inverter_hourly s
                        LEFT JOIN gold.inverter_daily g 
                            ON s.plant_id = g.plant_id 
                            AND s.inverter_id = g.inverter_id
                            AND date_trunc('day', s.timestamp_hour)::date = g.date
                        WHERE g.id IS NULL
                        ORDER BY day
                        LIMIT 100
                    """)
                    return cur.fetchall()
        except Exception as e:
            logger.error(f"Error getting unaggregated inverter dates: {e}")
            return []
    
    def get_unaggregated_meteo_hours(self) -> list:
        """Get distinct (plant_id, hour) from silver meteo that aren't in gold yet."""
        try:
            with DatabaseConnection(self.db_config) as db:
                with db.cursor() as cur:
                    cur.execute("""
                        SELECT DISTINCT s.plant_id, date_trunc('hour', s.timestamp) as hour
                        FROM silver.meteo_station_cleaned s
                        LEFT JOIN gold.meteo_station_hourly g 
                            ON s.plant_id = g.plant_id 
                            AND date_trunc('hour', s.timestamp) = g.timestamp_hour
                        WHERE g.id IS NULL
                        ORDER BY hour
                        LIMIT 200
                    """)
                    return cur.fetchall()
        except Exception as e:
            logger.error(f"Error getting unaggregated meteo hours: {e}")
            return []
    
    def get_unaggregated_meteo_dates(self) -> list:
        """Get distinct (plant_id, date) from gold hourly meteo that aren't in gold daily yet."""
        try:
            with DatabaseConnection(self.db_config) as db:
                with db.cursor() as cur:
                    cur.execute("""
                        SELECT DISTINCT s.plant_id, date_trunc('day', s.timestamp_hour)::date as day
                        FROM gold.meteo_station_hourly s
                        LEFT JOIN gold.meteo_station_daily g 
                            ON s.plant_id = g.plant_id 
                            AND date_trunc('day', s.timestamp_hour)::date = g.date
                        WHERE g.id IS NULL
                        ORDER BY day
                        LIMIT 100
                    """)
                    return cur.fetchall()
        except Exception as e:
            logger.error(f"Error getting unaggregated meteo dates: {e}")
            return []
    
    def get_unaggregated_poi_meter_hours(self) -> list:
        """Get distinct (plant_id, hour) from silver POI meter that aren't in gold yet."""
        try:
            with DatabaseConnection(self.db_config) as db:
                with db.cursor() as cur:
                    cur.execute("""
                        SELECT DISTINCT s.plant_id, date_trunc('hour', s.timestamp) as hour
                        FROM silver.poi_meter_cleaned s
                        LEFT JOIN gold.poi_meter_hourly g 
                            ON s.plant_id = g.plant_id 
                            AND date_trunc('hour', s.timestamp) = g.timestamp_hour
                        WHERE g.id IS NULL
                        ORDER BY hour
                        LIMIT 200
                    """)
                    return cur.fetchall()
        except Exception as e:
            logger.error(f"Error getting unaggregated POI meter hours: {e}")
            return []
    
    def get_unaggregated_poi_meter_dates(self) -> list:
        """Get distinct (plant_id, date) from gold hourly POI meter that aren't in gold daily yet."""
        try:
            with DatabaseConnection(self.db_config) as db:
                with db.cursor() as cur:
                    cur.execute("""
                        SELECT DISTINCT s.plant_id, date_trunc('day', s.timestamp_hour)::date as day
                        FROM gold.poi_meter_hourly s
                        LEFT JOIN gold.poi_meter_daily g 
                            ON s.plant_id = g.plant_id 
                            AND date_trunc('day', s.timestamp_hour)::date = g.date
                        WHERE g.id IS NULL
                        ORDER BY day
                        LIMIT 100
                    """)
                    return cur.fetchall()
        except Exception as e:
            logger.error(f"Error getting unaggregated POI meter dates: {e}")
            return []
    
    def get_unaggregated_system_status_hours(self) -> list:
        """Get distinct (plant_id, hour) from silver system_status that aren't in gold yet."""
        try:
            with DatabaseConnection(self.db_config) as db:
                with db.cursor() as cur:
                    cur.execute("""
                        SELECT DISTINCT s.plant_id, date_trunc('hour', s.timestamp) as hour
                        FROM silver.system_status_cleaned s
                        LEFT JOIN gold.system_status_hourly g 
                            ON s.plant_id = g.plant_id 
                            AND date_trunc('hour', s.timestamp) = g.timestamp_hour
                        WHERE g.id IS NULL
                        ORDER BY hour
                        LIMIT 200
                    """)
                    return cur.fetchall()
        except Exception as e:
            logger.error(f"Error getting unaggregated system_status hours: {e}")
            return []
    
    def get_unaggregated_system_status_dates(self) -> list:
        """Get distinct (plant_id, date) from gold hourly system_status that aren't in gold daily yet."""
        try:
            with DatabaseConnection(self.db_config) as db:
                with db.cursor() as cur:
                    cur.execute("""
                        SELECT DISTINCT s.plant_id, date_trunc('day', s.timestamp_hour)::date as day
                        FROM gold.system_status_hourly s
                        LEFT JOIN gold.system_status_daily g 
                            ON s.plant_id = g.plant_id 
                            AND date_trunc('day', s.timestamp_hour)::date = g.date
                        WHERE g.id IS NULL
                        ORDER BY day
                        LIMIT 100
                    """)
                    return cur.fetchall()
        except Exception as e:
            logger.error(f"Error getting unaggregated system_status dates: {e}")
            return []
    
    def get_unaggregated_plant_summary_hours(self) -> list:
        """Get distinct (plant_id, hour) that have inverter hourly data but no plant summary yet."""
        try:
            with DatabaseConnection(self.db_config) as db:
                with db.cursor() as cur:
                    cur.execute("""
                        SELECT DISTINCT s.plant_id, s.timestamp_hour as hour
                        FROM gold.inverter_hourly s
                        LEFT JOIN gold.plant_hourly_summary g 
                            ON s.plant_id = g.plant_id 
                            AND s.timestamp_hour = g.timestamp_hour
                        WHERE g.id IS NULL
                        ORDER BY hour
                        LIMIT 200
                    """)
                    return cur.fetchall()
        except Exception as e:
            logger.error(f"Error getting unaggregated plant_summary hours: {e}")
            return []
    
    def get_unaggregated_plant_summary_dates(self) -> list:
        """Get distinct (plant_id, date) that have plant summary hourly but no daily yet."""
        try:
            with DatabaseConnection(self.db_config) as db:
                with db.cursor() as cur:
                    cur.execute("""
                        SELECT DISTINCT s.plant_id, date_trunc('day', s.timestamp_hour)::date as day
                        FROM gold.plant_hourly_summary s
                        LEFT JOIN gold.plant_daily_summary g 
                            ON s.plant_id = g.plant_id 
                            AND date_trunc('day', s.timestamp_hour)::date = g.date
                        WHERE g.id IS NULL
                        ORDER BY day
                        LIMIT 100
                    """)
                    return cur.fetchall()
        except Exception as e:
            logger.error(f"Error getting unaggregated plant_summary dates: {e}")
            return []

    # =========================================================================
    # INVERTER GOLD AGGREGATIONS
    # =========================================================================
    
    def aggregate_inverter_hourly(self, plant_id: str, inverter_id: str, 
                                   timestamp_hour: str) -> Optional[int]:
        """
        Aggregate inverter data for a specific hour.
        Assumes data comes at 1-minute intervals.
        
        Args:
            plant_id: UUID of the plant
            inverter_id: ID of the inverter
            timestamp_hour: Hour to aggregate (truncated timestamp)
        
        Returns:
            Gold record ID if successful, None otherwise
        """
        try:
            with DatabaseConnection(self.db_config) as db:
                with db.cursor(commit=True) as cur:
                    params = {
                        "plant_id": plant_id,
                        "inverter_id": inverter_id,
                        "timestamp_hour": timestamp_hour,
                    }
                    cur.execute(
                        """
                        INSERT INTO gold.inverter_hourly (
                            plant_id, inverter_id, timestamp_hour,
                            energy_kwh, avg_power_kw, max_power_kw, min_power_kw,
                            temp_avg_c, temp_min_c, temp_max_c,
                            freq_avg_hz, availability_percent, operating_minutes,
                            failure_count, failure_types, record_count, valid_record_count
                        )
                        SELECT 
                            plant_id,
                            inverter_id,
                            date_trunc('hour', timestamp) AS timestamp_hour,
                            -- Energy: integrate power over time (assuming 1-min intervals)
                            SUM(COALESCE(ac_power_kW, 0)) / 60.0 AS energy_kwh,
                            AVG(ac_power_kW) AS avg_power_kw,
                            MAX(ac_power_kW) AS max_power_kw,
                            MIN(ac_power_kW) AS min_power_kw,
                            AVG(inverter_temp_C) AS temp_avg_c,
                            MIN(inverter_temp_C) AS temp_min_c,
                            MAX(inverter_temp_C) AS temp_max_c,
                            AVG(ac_freq_Hz) AS freq_avg_hz,
                            -- Availability: percent of records where state = 1 (running)
                            (SUM(CASE WHEN state = 1 THEN 1 ELSE 0 END)::NUMERIC / 
                             NULLIF(COUNT(*), 0) * 100) AS availability_percent,
                            SUM(CASE WHEN state = 1 THEN 1 ELSE 0 END) AS operating_minutes,
                            SUM(COALESCE(active_failures, 0)) AS failure_count,
                            jsonb_agg(DISTINCT failure_types) FILTER (WHERE failure_types IS NOT NULL AND failure_types != '[]'::jsonb) AS failure_types,
                            COUNT(*) AS record_count,
                            SUM(CASE WHEN data_quality_flag = 'valid' THEN 1 ELSE 0 END) AS valid_record_count
                        FROM silver.inverter_cleaned
                                                WHERE plant_id = %(plant_id)s::uuid
                                                    AND inverter_id = %(inverter_id)s
                                                    AND date_trunc('hour', timestamp) = %(timestamp_hour)s::timestamptz
                        GROUP BY plant_id, inverter_id, date_trunc('hour', timestamp)
                        ON CONFLICT (plant_id, inverter_id, timestamp_hour) DO UPDATE SET
                            energy_kwh = EXCLUDED.energy_kwh,
                            avg_power_kw = EXCLUDED.avg_power_kw,
                            max_power_kw = EXCLUDED.max_power_kw,
                            min_power_kw = EXCLUDED.min_power_kw,
                            temp_avg_c = EXCLUDED.temp_avg_c,
                            temp_min_c = EXCLUDED.temp_min_c,
                            temp_max_c = EXCLUDED.temp_max_c,
                            freq_avg_hz = EXCLUDED.freq_avg_hz,
                            availability_percent = EXCLUDED.availability_percent,
                            operating_minutes = EXCLUDED.operating_minutes,
                            failure_count = EXCLUDED.failure_count,
                            failure_types = EXCLUDED.failure_types,
                            record_count = EXCLUDED.record_count,
                            valid_record_count = EXCLUDED.valid_record_count,
                            processed_timestamp = CURRENT_TIMESTAMP
                        RETURNING id
                        """,
                        params
                    )
                    result = cur.fetchone()
                    return result[0] if result else None
        except Error as e:
            logger.error(f"Failed to aggregate inverter hourly: {e}")
            raise e
    
    def aggregate_inverter_daily(self, plant_id: str, inverter_id: str, 
                                  target_date: str) -> Optional[int]:
        """
        Aggregate inverter data for a specific day.
        Uses hourly gold data if available, otherwise falls back to silver.
        """
        try:
            with DatabaseConnection(self.db_config) as db:
                with db.cursor(commit=True) as cur:
                    cur.execute(
                        """
                        INSERT INTO gold.inverter_daily (
                            plant_id, inverter_id, date,
                            total_energy_kwh, peak_power_kw, avg_power_kw, min_power_kw,
                            temp_avg_c, temp_min_c, temp_max_c,
                            freq_avg_hz, freq_min_hz, freq_max_hz,
                            availability_percent, operating_hours,
                            total_records, valid_records,
                            total_failures, failure_types_summary
                        )
                        SELECT 
                            plant_id,
                            inverter_id,
                            %s::date AS date,
                            SUM(energy_kwh) AS total_energy_kwh,
                            MAX(max_power_kw) AS peak_power_kw,
                            AVG(avg_power_kw) AS avg_power_kw,
                            MIN(min_power_kw) AS min_power_kw,
                            AVG(temp_avg_c) AS temp_avg_c,
                            MIN(temp_min_c) AS temp_min_c,
                            MAX(temp_max_c) AS temp_max_c,
                            AVG(freq_avg_hz) AS freq_avg_hz,
                            MIN(freq_avg_hz) AS freq_min_hz,
                            MAX(freq_avg_hz) AS freq_max_hz,
                            AVG(availability_percent) AS availability_percent,
                            SUM(operating_minutes) / 60.0 AS operating_hours,
                            SUM(record_count) AS total_records,
                            SUM(valid_record_count) AS valid_records,
                            SUM(failure_count) AS total_failures,
                            jsonb_agg(failure_types) FILTER (WHERE failure_types IS NOT NULL) AS failure_types_summary
                        FROM gold.inverter_hourly
                        WHERE plant_id = %s::uuid
                          AND inverter_id = %s
                          AND timestamp_hour::date = %s::date
                        GROUP BY plant_id, inverter_id
                        ON CONFLICT (plant_id, inverter_id, date) DO UPDATE SET
                            total_energy_kwh = EXCLUDED.total_energy_kwh,
                            peak_power_kw = EXCLUDED.peak_power_kw,
                            avg_power_kw = EXCLUDED.avg_power_kw,
                            min_power_kw = EXCLUDED.min_power_kw,
                            temp_avg_c = EXCLUDED.temp_avg_c,
                            temp_min_c = EXCLUDED.temp_min_c,
                            temp_max_c = EXCLUDED.temp_max_c,
                            freq_avg_hz = EXCLUDED.freq_avg_hz,
                            freq_min_hz = EXCLUDED.freq_min_hz,
                            freq_max_hz = EXCLUDED.freq_max_hz,
                            availability_percent = EXCLUDED.availability_percent,
                            operating_hours = EXCLUDED.operating_hours,
                            total_records = EXCLUDED.total_records,
                            valid_records = EXCLUDED.valid_records,
                            total_failures = EXCLUDED.total_failures,
                            failure_types_summary = EXCLUDED.failure_types_summary,
                            processed_timestamp = CURRENT_TIMESTAMP
                        RETURNING id
                        """,
                        (target_date, plant_id, inverter_id, target_date)
                    )
                    result = cur.fetchone()
                    return result[0] if result else None
        except Error as e:
            logger.error(f"Failed to aggregate inverter daily: {e}")
            raise e
    
    # =========================================================================
    # METEO STATION GOLD AGGREGATIONS
    # =========================================================================
    
    def aggregate_meteo_hourly(self, plant_id: str, timestamp_hour: str) -> Optional[int]:
        """Aggregate meteorological station data for a specific hour."""
        try:
            with DatabaseConnection(self.db_config) as db:
                with db.cursor(commit=True) as cur:
                    cur.execute(
                        """
                        INSERT INTO gold.meteo_station_hourly (
                            plant_id, timestamp_hour,
                            temp_avg_c, temp_min_c, temp_max_c,
                            module_temp_avg_c, module_temp_min_c, module_temp_max_c,
                            humidity_avg_percent, humidity_min_percent, humidity_max_percent,
                            wind_speed_avg_ms, wind_speed_max_ms, wind_dir_avg_deg,
                            irradiation_wh_m2, avg_irradiance_wm2, peak_irradiance_wm2,
                            record_count, valid_record_count
                        )
                        SELECT 
                            plant_id,
                            date_trunc('hour', timestamp) AS timestamp_hour,
                            AVG(amb_temp_C) AS temp_avg_c,
                            MIN(amb_temp_C) AS temp_min_c,
                            MAX(amb_temp_C) AS temp_max_c,
                            AVG(module_temp_C) AS module_temp_avg_c,
                            MIN(module_temp_C) AS module_temp_min_c,
                            MAX(module_temp_C) AS module_temp_max_c,
                            AVG(humidity_percent) AS humidity_avg_percent,
                            MIN(humidity_percent) AS humidity_min_percent,
                            MAX(humidity_percent) AS humidity_max_percent,
                            AVG(wind_speed_ms) AS wind_speed_avg_ms,
                            MAX(wind_speed_ms) AS wind_speed_max_ms,
                            AVG(wind_dir_deg) AS wind_dir_avg_deg,
                            -- Irradiation: integrate W/m² over time to Wh/m² (1-min intervals)
                            SUM(COALESCE(poa_irradiance_wm2, 0)) / 60.0 AS irradiation_wh_m2,
                            AVG(poa_irradiance_wm2) AS avg_irradiance_wm2,
                            MAX(poa_irradiance_wm2) AS peak_irradiance_wm2,
                            COUNT(*) AS record_count,
                            SUM(CASE WHEN data_quality_flag = 'valid' THEN 1 ELSE 0 END) AS valid_record_count
                        FROM silver.meteo_station_cleaned
                        WHERE plant_id = %s::uuid
                          AND date_trunc('hour', timestamp) = %s::timestamptz
                        GROUP BY plant_id, date_trunc('hour', timestamp)
                        ON CONFLICT (plant_id, timestamp_hour) DO UPDATE SET
                            temp_avg_c = EXCLUDED.temp_avg_c,
                            temp_min_c = EXCLUDED.temp_min_c,
                            temp_max_c = EXCLUDED.temp_max_c,
                            module_temp_avg_c = EXCLUDED.module_temp_avg_c,
                            module_temp_min_c = EXCLUDED.module_temp_min_c,
                            module_temp_max_c = EXCLUDED.module_temp_max_c,
                            humidity_avg_percent = EXCLUDED.humidity_avg_percent,
                            humidity_min_percent = EXCLUDED.humidity_min_percent,
                            humidity_max_percent = EXCLUDED.humidity_max_percent,
                            wind_speed_avg_ms = EXCLUDED.wind_speed_avg_ms,
                            wind_speed_max_ms = EXCLUDED.wind_speed_max_ms,
                            wind_dir_avg_deg = EXCLUDED.wind_dir_avg_deg,
                            irradiation_wh_m2 = EXCLUDED.irradiation_wh_m2,
                            avg_irradiance_wm2 = EXCLUDED.avg_irradiance_wm2,
                            peak_irradiance_wm2 = EXCLUDED.peak_irradiance_wm2,
                            record_count = EXCLUDED.record_count,
                            valid_record_count = EXCLUDED.valid_record_count,
                            processed_timestamp = CURRENT_TIMESTAMP
                        RETURNING id
                        """,
                        (plant_id, timestamp_hour)
                    )
                    result = cur.fetchone()
                    return result[0] if result else None
        except Error as e:
            logger.error(f"Failed to aggregate meteo hourly: {e}")
            raise e
    
    def aggregate_meteo_daily(self, plant_id: str, target_date: str) -> Optional[int]:
        """Aggregate meteorological station data for a specific day."""
        try:
            with DatabaseConnection(self.db_config) as db:
                with db.cursor(commit=True) as cur:
                    cur.execute(
                        """
                        INSERT INTO gold.meteo_station_daily (
                            plant_id, date,
                            temp_avg_c, temp_min_c, temp_max_c,
                            module_temp_avg_c, module_temp_min_c, module_temp_max_c,
                            humidity_avg_percent, humidity_min_percent, humidity_max_percent,
                            wind_speed_avg_ms, wind_speed_max_ms, predominant_wind_dir_deg,
                            total_irradiation_kwh_m2, peak_irradiance_wm2, avg_irradiance_wm2,
                            sunshine_hours, total_records, valid_records
                        )
                        SELECT 
                            plant_id,
                            %s::date AS date,
                            AVG(temp_avg_c) AS temp_avg_c,
                            MIN(temp_min_c) AS temp_min_c,
                            MAX(temp_max_c) AS temp_max_c,
                            AVG(module_temp_avg_c) AS module_temp_avg_c,
                            MIN(module_temp_min_c) AS module_temp_min_c,
                            MAX(module_temp_max_c) AS module_temp_max_c,
                            AVG(humidity_avg_percent) AS humidity_avg_percent,
                            MIN(humidity_min_percent) AS humidity_min_percent,
                            MAX(humidity_max_percent) AS humidity_max_percent,
                            AVG(wind_speed_avg_ms) AS wind_speed_avg_ms,
                            MAX(wind_speed_max_ms) AS wind_speed_max_ms,
                            AVG(wind_dir_avg_deg) AS predominant_wind_dir_deg,
                            -- Convert Wh/m² to kWh/m²
                            SUM(irradiation_wh_m2) / 1000.0 AS total_irradiation_kwh_m2,
                            MAX(peak_irradiance_wm2) AS peak_irradiance_wm2,
                            AVG(avg_irradiance_wm2) AS avg_irradiance_wm2,
                            -- Sunshine hours: hours with avg irradiance > 120 W/m²
                            SUM(CASE WHEN avg_irradiance_wm2 > 120 THEN 1 ELSE 0 END) AS sunshine_hours,
                            SUM(record_count) AS total_records,
                            SUM(valid_record_count) AS valid_records
                        FROM gold.meteo_station_hourly
                        WHERE plant_id = %s::uuid
                          AND timestamp_hour::date = %s::date
                        GROUP BY plant_id
                        ON CONFLICT (plant_id, date) DO UPDATE SET
                            temp_avg_c = EXCLUDED.temp_avg_c,
                            temp_min_c = EXCLUDED.temp_min_c,
                            temp_max_c = EXCLUDED.temp_max_c,
                            module_temp_avg_c = EXCLUDED.module_temp_avg_c,
                            module_temp_min_c = EXCLUDED.module_temp_min_c,
                            module_temp_max_c = EXCLUDED.module_temp_max_c,
                            humidity_avg_percent = EXCLUDED.humidity_avg_percent,
                            humidity_min_percent = EXCLUDED.humidity_min_percent,
                            humidity_max_percent = EXCLUDED.humidity_max_percent,
                            wind_speed_avg_ms = EXCLUDED.wind_speed_avg_ms,
                            wind_speed_max_ms = EXCLUDED.wind_speed_max_ms,
                            predominant_wind_dir_deg = EXCLUDED.predominant_wind_dir_deg,
                            total_irradiation_kwh_m2 = EXCLUDED.total_irradiation_kwh_m2,
                            peak_irradiance_wm2 = EXCLUDED.peak_irradiance_wm2,
                            avg_irradiance_wm2 = EXCLUDED.avg_irradiance_wm2,
                            sunshine_hours = EXCLUDED.sunshine_hours,
                            total_records = EXCLUDED.total_records,
                            valid_records = EXCLUDED.valid_records,
                            processed_timestamp = CURRENT_TIMESTAMP
                        RETURNING id
                        """,
                        (target_date, plant_id, target_date)
                    )
                    result = cur.fetchone()
                    return result[0] if result else None
        except Error as e:
            logger.error(f"Failed to aggregate meteo daily: {e}")
            raise e
    
    # =========================================================================
    # POI METER GOLD AGGREGATIONS
    # =========================================================================
    
    def aggregate_poi_meter_hourly(self, plant_id: str, timestamp_hour: str) -> Optional[int]:
        """Aggregate POI meter data for a specific hour."""
        try:
            with DatabaseConnection(self.db_config) as db:
                with db.cursor(commit=True) as cur:
                    params = {
                        "plant_id": plant_id,
                        "timestamp_hour": timestamp_hour,
                    }
                    cur.execute(
                        """
                        INSERT INTO gold.poi_meter_hourly (
                            plant_id, timestamp_hour,
                            export_energy_kwh, import_energy_kwh, reactive_kvarh,
                            avg_export_kw, max_export_kw, avg_import_kw, max_import_kw,
                            avg_reactive_kvar, voltage_l1_avg_v, voltage_l2_avg_v, voltage_l3_avg_v,
                            voltage_imbalance_pct, frequency_avg_hz, frequency_min_hz, frequency_max_hz,
                            frequency_deviation_count, power_factor_avg, power_factor_min,
                            availability_percent, connection_issue_minutes,
                            failure_count, record_count, valid_record_count
                        )
                        SELECT 
                            plant_id,
                            date_trunc('hour', timestamp) AS timestamp_hour,
                            -- Energy: integrate power over time (1-min intervals)
                            SUM(COALESCE(export_active_power_kW, 0)) / 60.0 AS export_energy_kwh,
                            SUM(COALESCE(import_active_power_kW, 0)) / 60.0 AS import_energy_kwh,
                            SUM(COALESCE(reactive_power_kVAr, 0)) / 60.0 AS reactive_kvarh,
                            AVG(NULLIF(export_active_power_kW, 0)) AS avg_export_kw,
                            MAX(export_active_power_kW) AS max_export_kw,
                            AVG(NULLIF(import_active_power_kW, 0)) AS avg_import_kw,
                            MAX(import_active_power_kW) AS max_import_kw,
                            AVG(reactive_power_kVAr) AS avg_reactive_kvar,
                            AVG(grid_voltage_l1_V) AS voltage_l1_avg_v,
                            AVG(grid_voltage_l2_V) AS voltage_l2_avg_v,
                            AVG(grid_voltage_l3_V) AS voltage_l3_avg_v,
                            -- Voltage imbalance: std dev / mean * 100
                            STDDEV(grid_voltage_l1_V + grid_voltage_l2_V + grid_voltage_l3_V) / 
                                NULLIF(AVG(grid_voltage_l1_V + grid_voltage_l2_V + grid_voltage_l3_V), 0) * 100 AS voltage_imbalance_pct,
                            AVG(grid_frequency_Hz) AS frequency_avg_hz,
                            MIN(grid_frequency_Hz) AS frequency_min_hz,
                            MAX(grid_frequency_Hz) AS frequency_max_hz,
                            SUM(CASE WHEN grid_frequency_Hz < 49.5 OR grid_frequency_Hz > 50.5 THEN 1 ELSE 0 END) AS frequency_deviation_count,
                            AVG(power_factor) AS power_factor_avg,
                            MIN(power_factor) AS power_factor_min,
                            -- Availability: percent of records without connection issues
                            (SUM(CASE WHEN NOT connection_issues THEN 1 ELSE 0 END)::NUMERIC / 
                             NULLIF(COUNT(*), 0) * 100) AS availability_percent,
                            SUM(CASE WHEN connection_issues THEN 1 ELSE 0 END) AS connection_issue_minutes,
                            SUM(COALESCE(active_failures, 0)) AS failure_count,
                            COUNT(*) AS record_count,
                            SUM(CASE WHEN data_quality_flag = 'valid' THEN 1 ELSE 0 END) AS valid_record_count
                        FROM silver.poi_meter_cleaned
                        WHERE plant_id = %(plant_id)s::uuid
                          AND date_trunc('hour', timestamp) = %(timestamp_hour)s::timestamptz
                        GROUP BY plant_id, date_trunc('hour', timestamp)
                        ON CONFLICT (plant_id, timestamp_hour) DO UPDATE SET
                            export_energy_kwh = EXCLUDED.export_energy_kwh,
                            import_energy_kwh = EXCLUDED.import_energy_kwh,
                            reactive_kvarh = EXCLUDED.reactive_kvarh,
                            avg_export_kw = EXCLUDED.avg_export_kw,
                            max_export_kw = EXCLUDED.max_export_kw,
                            avg_import_kw = EXCLUDED.avg_import_kw,
                            max_import_kw = EXCLUDED.max_import_kw,
                            avg_reactive_kvar = EXCLUDED.avg_reactive_kvar,
                            voltage_l1_avg_v = EXCLUDED.voltage_l1_avg_v,
                            voltage_l2_avg_v = EXCLUDED.voltage_l2_avg_v,
                            voltage_l3_avg_v = EXCLUDED.voltage_l3_avg_v,
                            voltage_imbalance_pct = EXCLUDED.voltage_imbalance_pct,
                            frequency_avg_hz = EXCLUDED.frequency_avg_hz,
                            frequency_min_hz = EXCLUDED.frequency_min_hz,
                            frequency_max_hz = EXCLUDED.frequency_max_hz,
                            frequency_deviation_count = EXCLUDED.frequency_deviation_count,
                            power_factor_avg = EXCLUDED.power_factor_avg,
                            power_factor_min = EXCLUDED.power_factor_min,
                            availability_percent = EXCLUDED.availability_percent,
                            connection_issue_minutes = EXCLUDED.connection_issue_minutes,
                            failure_count = EXCLUDED.failure_count,
                            record_count = EXCLUDED.record_count,
                            valid_record_count = EXCLUDED.valid_record_count,
                            processed_timestamp = CURRENT_TIMESTAMP
                        RETURNING id
                        """,
                        params
                    )
                    result = cur.fetchone()
                    return result[0] if result else None
        except Error as e:
            logger.error(f"Failed to aggregate POI meter hourly: {e}")
            raise e
    
    def aggregate_poi_meter_daily(self, plant_id: str, target_date: str) -> Optional[int]:
        """Aggregate POI meter data for a specific day from hourly data."""
        try:
            with DatabaseConnection(self.db_config) as db:
                with db.cursor(commit=True) as cur:
                    cur.execute(
                        """
                        INSERT INTO gold.poi_meter_daily (
                            plant_id, date,
                            total_export_kwh, total_import_kwh,
                            peak_export_kw, peak_import_kw, avg_export_kw, avg_import_kw,
                            total_reactive_kvarh, avg_reactive_kvar,
                            voltage_l1_avg_v, voltage_l2_avg_v, voltage_l3_avg_v,
                            voltage_imbalance_pct, voltage_deviation_minutes,
                            frequency_avg_hz, frequency_min_hz, frequency_max_hz, frequency_deviation_minutes,
                            power_factor_avg, power_factor_min, power_factor_max,
                            availability_percent, connection_issue_hours,
                            total_failures, total_records, valid_records
                        )
                        SELECT 
                            plant_id,
                            %s::date AS date,
                            SUM(export_energy_kwh) AS total_export_kwh,
                            SUM(import_energy_kwh) AS total_import_kwh,
                            MAX(max_export_kw) AS peak_export_kw,
                            MAX(max_import_kw) AS peak_import_kw,
                            AVG(avg_export_kw) AS avg_export_kw,
                            AVG(avg_import_kw) AS avg_import_kw,
                            SUM(reactive_kvarh) AS total_reactive_kvarh,
                            AVG(avg_reactive_kvar) AS avg_reactive_kvar,
                            AVG(voltage_l1_avg_v) AS voltage_l1_avg_v,
                            AVG(voltage_l2_avg_v) AS voltage_l2_avg_v,
                            AVG(voltage_l3_avg_v) AS voltage_l3_avg_v,
                            AVG(voltage_imbalance_pct) AS voltage_imbalance_pct,
                            SUM(CASE WHEN voltage_l1_avg_v < 360 OR voltage_l1_avg_v > 440 THEN 1 ELSE 0 END) AS voltage_deviation_minutes,
                            AVG(frequency_avg_hz) AS frequency_avg_hz,
                            MIN(frequency_min_hz) AS frequency_min_hz,
                            MAX(frequency_max_hz) AS frequency_max_hz,
                            SUM(frequency_deviation_count) AS frequency_deviation_minutes,
                            AVG(power_factor_avg) AS power_factor_avg,
                            MIN(power_factor_min) AS power_factor_min,
                            MAX(power_factor_avg) AS power_factor_max,
                            AVG(availability_percent) AS availability_percent,
                            SUM(connection_issue_minutes) / 60.0 AS connection_issue_hours,
                            SUM(failure_count) AS total_failures,
                            SUM(record_count) AS total_records,
                            SUM(valid_record_count) AS valid_records
                        FROM gold.poi_meter_hourly
                        WHERE plant_id = %s::uuid
                          AND timestamp_hour::date = %s::date
                        GROUP BY plant_id
                        ON CONFLICT (plant_id, date) DO UPDATE SET
                            total_export_kwh = EXCLUDED.total_export_kwh,
                            total_import_kwh = EXCLUDED.total_import_kwh,
                            peak_export_kw = EXCLUDED.peak_export_kw,
                            peak_import_kw = EXCLUDED.peak_import_kw,
                            avg_export_kw = EXCLUDED.avg_export_kw,
                            avg_import_kw = EXCLUDED.avg_import_kw,
                            total_reactive_kvarh = EXCLUDED.total_reactive_kvarh,
                            avg_reactive_kvar = EXCLUDED.avg_reactive_kvar,
                            voltage_l1_avg_v = EXCLUDED.voltage_l1_avg_v,
                            voltage_l2_avg_v = EXCLUDED.voltage_l2_avg_v,
                            voltage_l3_avg_v = EXCLUDED.voltage_l3_avg_v,
                            voltage_imbalance_pct = EXCLUDED.voltage_imbalance_pct,
                            voltage_deviation_minutes = EXCLUDED.voltage_deviation_minutes,
                            frequency_avg_hz = EXCLUDED.frequency_avg_hz,
                            frequency_min_hz = EXCLUDED.frequency_min_hz,
                            frequency_max_hz = EXCLUDED.frequency_max_hz,
                            frequency_deviation_minutes = EXCLUDED.frequency_deviation_minutes,
                            power_factor_avg = EXCLUDED.power_factor_avg,
                            power_factor_min = EXCLUDED.power_factor_min,
                            power_factor_max = EXCLUDED.power_factor_max,
                            availability_percent = EXCLUDED.availability_percent,
                            connection_issue_hours = EXCLUDED.connection_issue_hours,
                            total_failures = EXCLUDED.total_failures,
                            total_records = EXCLUDED.total_records,
                            valid_records = EXCLUDED.valid_records,
                            processed_timestamp = CURRENT_TIMESTAMP
                        RETURNING id
                        """,
                        (target_date, plant_id, target_date)
                    )
                    result = cur.fetchone()
                    return result[0] if result else None
        except Error as e:
            logger.error(f"Failed to aggregate POI meter daily: {e}")
            raise e
    
    # =========================================================================
    # SYSTEM STATUS GOLD AGGREGATIONS
    # =========================================================================
    
    def aggregate_system_status_hourly(self, plant_id: str, timestamp_hour: str) -> Optional[int]:
        """Aggregate system status data for a specific hour."""
        try:
            with DatabaseConnection(self.db_config) as db:
                with db.cursor(commit=True) as cur:
                    cur.execute(
                        """
                        INSERT INTO gold.system_status_hourly (
                            plant_id, timestamp_hour,
                            total_failures, max_concurrent_failures, avg_failures,
                            stress_level_avg, stress_level_max, stress_level_min,
                            avg_healthy_components, min_healthy_components,
                            avg_failed_components, max_failed_components,
                            critical_failure_count, max_concurrent_critical,
                            record_count, valid_record_count
                        )
                        SELECT 
                            plant_id,
                            date_trunc('hour', timestamp) AS timestamp_hour,
                            SUM(total_failures) AS total_failures,
                            MAX(total_failures) AS max_concurrent_failures,
                            AVG(total_failures) AS avg_failures,
                            AVG(stress_level) AS stress_level_avg,
                            MAX(stress_level) AS stress_level_max,
                            MIN(stress_level) AS stress_level_min,
                            AVG(healthy_components) AS avg_healthy_components,
                            MIN(healthy_components) AS min_healthy_components,
                            AVG(failed_components) AS avg_failed_components,
                            MAX(failed_components) AS max_failed_components,
                            SUM(critical_failures) AS critical_failure_count,
                            MAX(critical_failures) AS max_concurrent_critical,
                            COUNT(*) AS record_count,
                            SUM(CASE WHEN data_quality_flag = 'valid' THEN 1 ELSE 0 END) AS valid_record_count
                        FROM silver.system_status_cleaned
                        WHERE plant_id = %s::uuid
                          AND date_trunc('hour', timestamp) = %s::timestamptz
                        GROUP BY plant_id, date_trunc('hour', timestamp)
                        ON CONFLICT (plant_id, timestamp_hour) DO UPDATE SET
                            total_failures = EXCLUDED.total_failures,
                            max_concurrent_failures = EXCLUDED.max_concurrent_failures,
                            avg_failures = EXCLUDED.avg_failures,
                            stress_level_avg = EXCLUDED.stress_level_avg,
                            stress_level_max = EXCLUDED.stress_level_max,
                            stress_level_min = EXCLUDED.stress_level_min,
                            avg_healthy_components = EXCLUDED.avg_healthy_components,
                            min_healthy_components = EXCLUDED.min_healthy_components,
                            avg_failed_components = EXCLUDED.avg_failed_components,
                            max_failed_components = EXCLUDED.max_failed_components,
                            critical_failure_count = EXCLUDED.critical_failure_count,
                            max_concurrent_critical = EXCLUDED.max_concurrent_critical,
                            record_count = EXCLUDED.record_count,
                            valid_record_count = EXCLUDED.valid_record_count,
                            processed_timestamp = CURRENT_TIMESTAMP
                        RETURNING id
                        """,
                        (plant_id, timestamp_hour)
                    )
                    result = cur.fetchone()
                    return result[0] if result else None
        except Error as e:
            logger.error(f"Failed to aggregate system status hourly: {e}")
            raise e
    
    def aggregate_system_status_daily(self, plant_id: str, target_date: str) -> Optional[int]:
        """Aggregate system status data for a specific day from hourly data."""
        try:
            with DatabaseConnection(self.db_config) as db:
                with db.cursor(commit=True) as cur:
                    cur.execute(
                        """
                        INSERT INTO gold.system_status_daily (
                            plant_id, date,
                            total_failures_sum, max_concurrent_failures, avg_failures,
                            stress_level_avg, stress_level_max, stress_level_min,
                            avg_healthy_components, min_healthy_components,
                            avg_failed_components, max_failed_components,
                            total_critical_failures, max_concurrent_critical,
                            total_records, valid_records
                        )
                        SELECT 
                            plant_id,
                            %s::date AS date,
                            SUM(total_failures) AS total_failures_sum,
                            MAX(max_concurrent_failures) AS max_concurrent_failures,
                            AVG(avg_failures) AS avg_failures,
                            AVG(stress_level_avg) AS stress_level_avg,
                            MAX(stress_level_max) AS stress_level_max,
                            MIN(stress_level_min) AS stress_level_min,
                            AVG(avg_healthy_components) AS avg_healthy_components,
                            MIN(min_healthy_components) AS min_healthy_components,
                            AVG(avg_failed_components) AS avg_failed_components,
                            MAX(max_failed_components) AS max_failed_components,
                            SUM(critical_failure_count) AS total_critical_failures,
                            MAX(max_concurrent_critical) AS max_concurrent_critical,
                            SUM(record_count) AS total_records,
                            SUM(valid_record_count) AS valid_records
                        FROM gold.system_status_hourly
                        WHERE plant_id = %s::uuid
                          AND timestamp_hour::date = %s::date
                        GROUP BY plant_id
                        ON CONFLICT (plant_id, date) DO UPDATE SET
                            total_failures_sum = EXCLUDED.total_failures_sum,
                            max_concurrent_failures = EXCLUDED.max_concurrent_failures,
                            avg_failures = EXCLUDED.avg_failures,
                            stress_level_avg = EXCLUDED.stress_level_avg,
                            stress_level_max = EXCLUDED.stress_level_max,
                            stress_level_min = EXCLUDED.stress_level_min,
                            avg_healthy_components = EXCLUDED.avg_healthy_components,
                            min_healthy_components = EXCLUDED.min_healthy_components,
                            avg_failed_components = EXCLUDED.avg_failed_components,
                            max_failed_components = EXCLUDED.max_failed_components,
                            total_critical_failures = EXCLUDED.total_critical_failures,
                            max_concurrent_critical = EXCLUDED.max_concurrent_critical,
                            total_records = EXCLUDED.total_records,
                            valid_records = EXCLUDED.valid_records,
                            processed_timestamp = CURRENT_TIMESTAMP
                        RETURNING id
                        """,
                        (target_date, plant_id, target_date)
                    )
                    result = cur.fetchone()
                    return result[0] if result else None
        except Error as e:
            logger.error(f"Failed to aggregate system status daily: {e}")
            raise e
    
    # =========================================================================
    # PLANT SUMMARY AGGREGATIONS (Cross-component)
    # =========================================================================
    
    def aggregate_plant_hourly_summary(self, plant_id: str, timestamp_hour: str) -> Optional[int]:
        """
        Aggregate plant-level hourly summary combining all components.
        Calculates Performance Ratio using plant capacity.
        
        Performance Ratio (PR) = E_actual / E_theoretical
        where E_theoretical = G_poa (kWh/m²) × Plant_capacity (kWp)
        
        For hourly: PR = generation_kwh / (irradiation_wh_m2 / 1000 × capacity_kwp)
        """
        try:
            with DatabaseConnection(self.db_config) as db:
                with db.cursor(commit=True) as cur:
                    cur.execute(
                        """
                        INSERT INTO gold.plant_hourly_summary (
                            plant_id, timestamp_hour,
                            total_generation_kwh, total_consumption_kwh, net_energy_kwh,
                            self_consumption_kwh, grid_export_kwh, grid_import_kwh,
                            total_power_kw, import_kw,
                            irradiation_wh_m2, avg_irradiance_wm2,
                            instantaneous_pr,
                            avg_ambient_temp_c, avg_module_temp_c,
                            active_inverters, total_inverters, failure_count,
                            system_stress_avg, data_completeness_percent
                        )
                        SELECT 
                            inv.plant_id,
                            inv.timestamp_hour,
                            inv.total_generation_kwh,
                            COALESCE(poi.import_energy_kwh, 0) AS total_consumption_kwh,
                            inv.total_generation_kwh - COALESCE(poi.import_energy_kwh, 0) AS net_energy_kwh,
                            LEAST(inv.total_generation_kwh, COALESCE(poi.import_energy_kwh, 0)) AS self_consumption_kwh,
                            COALESCE(poi.export_energy_kwh, 0) AS grid_export_kwh,
                            COALESCE(poi.import_energy_kwh, 0) AS grid_import_kwh,
                            inv.total_power_kw,
                            COALESCE(poi.avg_import_kw, 0) AS import_kw,
                            COALESCE(met.irradiation_wh_m2, 0) AS irradiation_wh_m2,
                            COALESCE(met.avg_irradiance_wm2, 0) AS avg_irradiance_wm2,
                            -- Performance Ratio: PR = E_actual / (G_poa × P_capacity)
                            -- G_poa in Wh/m² needs to be converted to kWh/m², capacity = 1260 kWp
                            CASE 
                                WHEN COALESCE(met.irradiation_wh_m2, 0) > 0 THEN 
                                    LEAST(1.0, inv.total_generation_kwh / 
                                        ((met.irradiation_wh_m2 / 1000.0) * %s))
                                ELSE NULL 
                            END AS instantaneous_pr,
                            met.temp_avg_c AS avg_ambient_temp_c,
                            met.module_temp_avg_c AS avg_module_temp_c,
                            inv.active_inverters,
                            inv.total_inverters,
                            inv.failure_count,
                            COALESCE(sys.stress_level_avg, 0) AS system_stress_avg,
                            CASE 
                                WHEN inv.total_inverters > 0 THEN 
                                    (inv.active_inverters::NUMERIC / inv.total_inverters * 100)
                                ELSE 100 
                            END AS data_completeness_percent
                        FROM (
                            SELECT 
                                plant_id,
                                timestamp_hour,
                                SUM(energy_kwh) AS total_generation_kwh,
                                SUM(avg_power_kw) AS total_power_kw,
                                COUNT(*) FILTER (WHERE energy_kwh > 0) AS active_inverters,
                                COUNT(*) AS total_inverters,
                                SUM(failure_count) AS failure_count
                            FROM gold.inverter_hourly
                            WHERE plant_id = %s::uuid AND timestamp_hour = %s::timestamptz
                            GROUP BY plant_id, timestamp_hour
                        ) inv
                        LEFT JOIN gold.meteo_station_hourly met 
                            ON inv.plant_id = met.plant_id AND inv.timestamp_hour = met.timestamp_hour
                        LEFT JOIN gold.poi_meter_hourly poi 
                            ON inv.plant_id = poi.plant_id AND inv.timestamp_hour = poi.timestamp_hour
                        LEFT JOIN (
                            SELECT 
                                plant_id,
                                date_trunc('hour', timestamp) AS timestamp_hour,
                                AVG(stress_level) AS stress_level_avg
                            FROM silver.system_status_cleaned
                            WHERE plant_id = %s::uuid 
                              AND date_trunc('hour', timestamp) = %s::timestamptz
                            GROUP BY plant_id, date_trunc('hour', timestamp)
                        ) sys ON inv.plant_id = sys.plant_id AND inv.timestamp_hour = sys.timestamp_hour
                        ON CONFLICT (plant_id, timestamp_hour) DO UPDATE SET
                            total_generation_kwh = EXCLUDED.total_generation_kwh,
                            total_consumption_kwh = EXCLUDED.total_consumption_kwh,
                            net_energy_kwh = EXCLUDED.net_energy_kwh,
                            self_consumption_kwh = EXCLUDED.self_consumption_kwh,
                            grid_export_kwh = EXCLUDED.grid_export_kwh,
                            grid_import_kwh = EXCLUDED.grid_import_kwh,
                            total_power_kw = EXCLUDED.total_power_kw,
                            import_kw = EXCLUDED.import_kw,
                            irradiation_wh_m2 = EXCLUDED.irradiation_wh_m2,
                            avg_irradiance_wm2 = EXCLUDED.avg_irradiance_wm2,
                            instantaneous_pr = EXCLUDED.instantaneous_pr,
                            avg_ambient_temp_c = EXCLUDED.avg_ambient_temp_c,
                            avg_module_temp_c = EXCLUDED.avg_module_temp_c,
                            active_inverters = EXCLUDED.active_inverters,
                            total_inverters = EXCLUDED.total_inverters,
                            failure_count = EXCLUDED.failure_count,
                            system_stress_avg = EXCLUDED.system_stress_avg,
                            data_completeness_percent = EXCLUDED.data_completeness_percent,
                            processed_timestamp = CURRENT_TIMESTAMP
                        RETURNING id
                        """,
                        (self.PLANT_CAPACITY_KWP, plant_id, timestamp_hour, plant_id, timestamp_hour)
                    )
                    result = cur.fetchone()
                    return result[0] if result else None
        except Error as e:
            logger.error(f"Failed to aggregate plant hourly summary: {e}")
            raise e
    
    def aggregate_plant_daily_summary(self, plant_id: str, target_date: str) -> Optional[int]:
        """
        Aggregate plant-level daily summary combining all components.
        Calculates key performance indicators using plant capacity.
        
        Formulas:
        - Specific Yield (Yf) = E_generation / P_capacity (kWh/kWp)
        - Performance Ratio (PR) = E_actual / E_theoretical
          where E_theoretical = G_poa (kWh/m²) × P_capacity (kWp)
        - Capacity Factor (CF) = E_actual / (P_capacity × 24h)
        - Temp Loss Factor = 1 + (T_module - T_STC) × temp_coefficient
        
        Plant capacity: 1,260 kWp (14 inverters × 90 kWp)
        """
        try:
            with DatabaseConnection(self.db_config) as db:
                with db.cursor(commit=True) as cur:
                    cur.execute(
                        """
                        INSERT INTO gold.plant_daily_summary (
                            plant_id, date,
                            total_generation_kwh, total_consumption_kwh, net_energy_kwh,
                            self_consumption_kwh, self_consumption_ratio, grid_export_kwh, grid_import_kwh,
                            total_irradiation_kwh_m2, sunshine_hours,
                            specific_yield_kwh_kwp, performance_ratio, capacity_factor,
                            avg_ambient_temp_c, avg_module_temp_c, temp_loss_factor,
                            plant_availability_percent, inverter_availability_avg,
                            total_failures, critical_failures, inverter_failures,
                            system_stress_avg, active_inverters, total_inverters,
                            data_completeness_percent
                        )
                        SELECT 
                            inv.plant_id,
                            inv.date,
                            inv.total_generation_kwh,
                            COALESCE(poi.total_import_kwh, 0) AS total_consumption_kwh,
                            inv.total_generation_kwh - COALESCE(poi.total_import_kwh, 0) AS net_energy_kwh,
                            LEAST(inv.total_generation_kwh, COALESCE(poi.total_import_kwh, 0)) AS self_consumption_kwh,
                            CASE 
                                WHEN inv.total_generation_kwh > 0 THEN 
                                    LEAST(inv.total_generation_kwh, COALESCE(poi.total_import_kwh, 0)) / inv.total_generation_kwh
                                ELSE 0 
                            END AS self_consumption_ratio,
                            COALESCE(poi.total_export_kwh, 0) AS grid_export_kwh,
                            COALESCE(poi.total_import_kwh, 0) AS grid_import_kwh,
                            COALESCE(met.total_irradiation_kwh_m2, 0) AS total_irradiation_kwh_m2,
                            COALESCE(met.sunshine_hours, 0) AS sunshine_hours,
                            -- Specific Yield: Yf = E_generation / P_capacity (kWh/kWp)
                            inv.total_generation_kwh / %s AS specific_yield_kwh_kwp,
                            -- Performance Ratio: PR = E_actual / (G_poa × P_capacity)
                            CASE 
                                WHEN COALESCE(met.total_irradiation_kwh_m2, 0) > 0 THEN 
                                    LEAST(1.0, inv.total_generation_kwh / 
                                        (met.total_irradiation_kwh_m2 * %s))
                                ELSE NULL 
                            END AS performance_ratio,
                            -- Capacity Factor: CF = E_actual / (P_capacity × 24h)
                            inv.total_generation_kwh / (%s * 24.0) AS capacity_factor,
                            met.temp_avg_c AS avg_ambient_temp_c,
                            met.module_temp_avg_c AS avg_module_temp_c,
                            -- Temperature Loss Factor: 1 + (T_module - 25°C) × (-0.004)
                            CASE 
                                WHEN met.module_temp_avg_c IS NOT NULL THEN 
                                    1.0 + (met.module_temp_avg_c - %s) * %s
                                ELSE NULL 
                            END AS temp_loss_factor,
                            COALESCE(poi.availability_percent, 100) AS plant_availability_percent,
                            inv.availability_avg AS inverter_availability_avg,
                            COALESCE(sys.total_failures_sum, 0) + inv.total_failures AS total_failures,
                            COALESCE(sys.total_critical_failures, 0) AS critical_failures,
                            inv.total_failures AS inverter_failures,
                            COALESCE(sys.stress_level_avg, 0) AS system_stress_avg,
                            inv.active_inverters,
                            inv.total_inverters,
                            CASE 
                                WHEN inv.total_inverters > 0 THEN 
                                    (inv.active_inverters::NUMERIC / inv.total_inverters * 100)
                                ELSE 100 
                            END AS data_completeness_percent
                        FROM (
                            SELECT 
                                plant_id,
                                date,
                                SUM(total_energy_kwh) AS total_generation_kwh,
                                AVG(availability_percent) AS availability_avg,
                                COUNT(*) FILTER (WHERE total_energy_kwh > 0) AS active_inverters,
                                COUNT(*) AS total_inverters,
                                SUM(total_failures) AS total_failures
                            FROM gold.inverter_daily
                            WHERE plant_id = %s::uuid AND date = %s::date
                            GROUP BY plant_id, date
                        ) inv
                        LEFT JOIN gold.meteo_station_daily met 
                            ON inv.plant_id = met.plant_id AND inv.date = met.date
                        LEFT JOIN gold.poi_meter_daily poi 
                            ON inv.plant_id = poi.plant_id AND inv.date = poi.date
                        LEFT JOIN gold.system_status_daily sys 
                            ON inv.plant_id = sys.plant_id AND inv.date = sys.date
                        ON CONFLICT (plant_id, date) DO UPDATE SET
                            total_generation_kwh = EXCLUDED.total_generation_kwh,
                            total_consumption_kwh = EXCLUDED.total_consumption_kwh,
                            net_energy_kwh = EXCLUDED.net_energy_kwh,
                            self_consumption_kwh = EXCLUDED.self_consumption_kwh,
                            self_consumption_ratio = EXCLUDED.self_consumption_ratio,
                            grid_export_kwh = EXCLUDED.grid_export_kwh,
                            grid_import_kwh = EXCLUDED.grid_import_kwh,
                            total_irradiation_kwh_m2 = EXCLUDED.total_irradiation_kwh_m2,
                            sunshine_hours = EXCLUDED.sunshine_hours,
                            specific_yield_kwh_kwp = EXCLUDED.specific_yield_kwh_kwp,
                            performance_ratio = EXCLUDED.performance_ratio,
                            capacity_factor = EXCLUDED.capacity_factor,
                            avg_ambient_temp_c = EXCLUDED.avg_ambient_temp_c,
                            avg_module_temp_c = EXCLUDED.avg_module_temp_c,
                            temp_loss_factor = EXCLUDED.temp_loss_factor,
                            plant_availability_percent = EXCLUDED.plant_availability_percent,
                            inverter_availability_avg = EXCLUDED.inverter_availability_avg,
                            total_failures = EXCLUDED.total_failures,
                            critical_failures = EXCLUDED.critical_failures,
                            inverter_failures = EXCLUDED.inverter_failures,
                            system_stress_avg = EXCLUDED.system_stress_avg,
                            active_inverters = EXCLUDED.active_inverters,
                            total_inverters = EXCLUDED.total_inverters,
                            data_completeness_percent = EXCLUDED.data_completeness_percent,
                            processed_timestamp = CURRENT_TIMESTAMP
                        RETURNING id
                        """,
                        (
                            self.PLANT_CAPACITY_KWP,      # specific_yield denominator
                            self.PLANT_CAPACITY_KWP,      # performance_ratio denominator
                            self.PLANT_CAPACITY_KWP,      # capacity_factor denominator
                            self.STC_TEMPERATURE,         # temp_loss_factor: STC temp (25°C)
                            self.MODULE_TEMP_COEFFICIENT, # temp_loss_factor: coefficient (-0.004)
                            plant_id, 
                            target_date
                        )
                    )
                    result = cur.fetchone()
                    return result[0] if result else None
        except Error as e:
            logger.error(f"Failed to aggregate plant daily summary: {e}")
            raise e
    
    # =========================================================================
    # HELPER METHODS
    # =========================================================================
    
    def get_distinct_plants(self) -> list:
        """Get list of distinct plant IDs from silver layer."""
        try:
            with DatabaseConnection(self.db_config) as db:
                with db.cursor() as cur:
                    cur.execute(
                        """
                        SELECT DISTINCT plant_id FROM silver.inverter_cleaned
                        UNION
                        SELECT DISTINCT plant_id FROM silver.meteo_station_cleaned
                        UNION
                        SELECT DISTINCT plant_id FROM silver.poi_meter_cleaned
                        """
                    )
                    return [str(row[0]) for row in cur.fetchall()]
        except Error as e:
            logger.error(f"Failed to get distinct plants: {e}")
            return []
    
    def get_distinct_inverters(self, plant_id: str) -> list:
        """Get list of distinct inverter IDs for a plant."""
        try:
            with DatabaseConnection(self.db_config) as db:
                with db.cursor() as cur:
                    cur.execute(
                        """
                        SELECT DISTINCT inverter_id 
                        FROM silver.inverter_cleaned
                        WHERE plant_id = %s::uuid
                        """,
                        (plant_id,)
                    )
                    return [row[0] for row in cur.fetchall()]
        except Error as e:
            logger.error(f"Failed to get distinct inverters: {e}")
            return []
    
    def get_hours_with_data(self, plant_id: str, target_date: str) -> list:
        """Get list of hours with data for a specific date."""
        try:
            with DatabaseConnection(self.db_config) as db:
                with db.cursor() as cur:
                    cur.execute(
                        """
                        SELECT DISTINCT date_trunc('hour', timestamp) AS hour
                        FROM silver.inverter_cleaned
                        WHERE plant_id = %s::uuid
                          AND timestamp::date = %s::date
                        ORDER BY hour
                        """,
                        (plant_id, target_date)
                    )
                    return [row[0].isoformat() for row in cur.fetchall()]
        except Error as e:
            logger.error(f"Failed to get hours with data: {e}")
            return []