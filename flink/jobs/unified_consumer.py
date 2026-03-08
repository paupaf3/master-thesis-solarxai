"""
Unified Kafka Consumer for SolarX.ai
Consumes from all topics (inverter, meteo, poi_meter, system_status) 
and stores data in PostgreSQL bronze layer.
Single job with higher parallelism for better memory efficiency.
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import MapFunction
import json
from datetime import datetime
import sys
sys.path.append('/opt/flink/jobs')
from utils.db_utils import DatabaseConfig, BronzeLayerWriter


class UnifiedWriteFunction(MapFunction):
    """Writes data from any topic to PostgreSQL bronze layer based on message type"""
    
    def __init__(self):
        self.writer = None
    
    def open(self, runtime_context):
        """Initialize database writer"""
        db_config = DatabaseConfig()
        self.writer = BronzeLayerWriter(db_config)
        print("[UNIFIED] Initialized BronzeLayerWriter")
    
    def map(self, value):
        try:
            data = json.loads(value)
            
            # Determine message type based on fields
            msg_type = self._detect_message_type(data)
            
            if msg_type == 'inverter':
                return self._process_inverter(data)
            elif msg_type == 'meteo':
                return self._process_meteo(data)
            elif msg_type == 'poi_meter':
                return self._process_poi_meter(data)
            elif msg_type == 'system_status':
                return self._process_system_status(data)
            else:
                return f"[UNIFIED] {datetime.now().isoformat()} | Unknown message type: {value[:100]}"
                
        except Exception as e:
            output = f"[UNIFIED] {datetime.now().isoformat()} | Error: {e} | Raw: {value[:200]}"
            print(output)
            return output
    
    def _detect_message_type(self, data: dict) -> str:
        """Detect message type based on characteristic fields"""
        if 'inverter_id' in data:
            return 'inverter'
        elif 'poa_irradiance_wm2' in data or 'amb_temp_C' in data:
            return 'meteo'
        elif 'export_active_power_kW' in data or 'import_active_power_kW' in data:
            return 'poi_meter'
        elif 'system_impact' in data or 'component_status' in data:
            return 'system_status'
        return 'unknown'
    
    def _process_inverter(self, data: dict) -> str:
        """Process inverter data"""
        row_id = self.writer.insert_inverter_data({
            'guid': data.get('guid'),
            'inverter_id': data.get('inverter_id'),
            'timestamp': data.get('timestamp'),
            'state': data.get('state'),
            'inverter_temp_C': data.get('inverter_temp_C'),
            'ac_power_kW': data.get('ac_power_kW'),
            'ac_freq_Hz': data.get('ac_freq_Hz'),
            'dc_power_kW': data.get('dc_power_kW'),
            'dc_voltage_V': data.get('dc_voltage_V'),
            'dc_current_A': data.get('dc_current_A'),
            'healthy_strings': data.get('healthy_strings'),
            'failed_strings': data.get('failed_strings'),
            'active_failures': data.get('active_failures'),
            'failure_types': data.get('failure_types')
        })
        
        output = f"[INVERTER] {datetime.now().isoformat()} | "
        output += f"ID: {data.get('inverter_id', 'N/A')} | "
        output += f"Power: {data.get('ac_power_kW', 'N/A')}kW | "
        output += f"Freq: {data.get('ac_freq_Hz', 'N/A')}Hz | "
        output += f"DC: {data.get('dc_power_kW', 'N/A')}kW @ {data.get('dc_voltage_V', 'N/A')}V | "
        output += f"Strings OK: {data.get('healthy_strings', 'N/A')}/{data.get('healthy_strings', 'N/A') + data.get('failed_strings', 0) if data.get('healthy_strings') is not None else 'N/A'} | "
        output += f"DB ID: {row_id}"
        print(output)
        return output
    
    def _process_meteo(self, data: dict) -> str:
        """Process meteorological data"""
        row_id = self.writer.insert_meteo_data({
            'guid': data.get('guid'),
            'timestamp': data.get('timestamp'),
            'amb_temp_C': data.get('amb_temp_C'),
            'module_temp_C': data.get('module_temp_C'),
            'wind_speed_ms': data.get('wind_speed_ms'),
            'wind_dir_deg': data.get('wind_dir_deg'),
            'humidity_percent': data.get('humidity_percent'),
            'poa_irradiance_wm2': data.get('poa_irradiance_wm2')
        })
        
        output = f"[METEO] {datetime.now().isoformat()} | "
        output += f"Irradiance: {data.get('poa_irradiance_wm2', 'N/A')}W/m² | "
        output += f"DB ID: {row_id}"
        print(output)
        return output
    
    def _process_poi_meter(self, data: dict) -> str:
        """Process POI meter data"""
        row_id = self.writer.insert_poi_meter_data({
            'guid': data.get('guid'),
            'timestamp': data.get('timestamp'),
            'export_active_power_kW': data.get('export_active_power_kW'),
            'import_active_power_kW': data.get('import_active_power_kW'),
            'reactive_power_kVAr': data.get('reactive_power_kVAr'),
            'grid_voltage_l1_V': data.get('grid_voltage_l1_V'),
            'grid_voltage_l2_V': data.get('grid_voltage_l2_V'),
            'grid_voltage_l3_V': data.get('grid_voltage_l3_V'),
            'grid_frequency_Hz': data.get('grid_frequency_Hz'),
            'power_factor': data.get('power_factor'),
            'active_failures': data.get('active_failures'),
            'connection_issues': data.get('connection_issues')
        })
        
        output = f"[POI_METER] {datetime.now().isoformat()} | "
        if data.get('export_active_power_kW', 0) > 0:
            output += f"Export: {data.get('export_active_power_kW', 'N/A')}kW | "
        else:
            output += f"Import: {data.get('import_active_power_kW', 'N/A')}kW | "
        output += f"Freq: {data.get('grid_frequency_Hz', 'N/A')}Hz | "
        output += f"DB ID: {row_id}"
        print(output)
        return output
    
    def _process_system_status(self, data: dict) -> str:
        """Process system status data"""
        row_id = self.writer.insert_system_status_data(data)
        
        impact = data.get('system_impact', {})
        total_failures = impact.get('total_failures', 0)
        
        output = f"[SYSTEM_STATUS] {datetime.now().isoformat()} | "
        output += f"Failures: {total_failures} | "
        output += f"DB ID: {row_id}"
        print(output)
        return output


def main():
    """Main entry point for unified consumer"""
    
    KAFKA_BROKERS = "broker-1:29092,broker-2:29192,broker-3:29292"
    TOPICS = ["inverter", "on_site_meteo_station", "poi_meter", "system_status"]
    
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(3)  # Higher parallelism for unified job
    
    print(f"[INFO] Starting Unified Kafka Consumer")
    print(f"[INFO] Topics: {TOPICS}")
    print(f"[INFO] Kafka Brokers: {KAFKA_BROKERS}")
    print(f"[INFO] Target Database: PostgreSQL (bronze layer)")
    print("[INFO] " + "="*60)
    
    kafka_consumer = FlinkKafkaConsumer(
        TOPICS,
        SimpleStringSchema(),
        {
            'bootstrap.servers': KAFKA_BROKERS,
            'group.id': 'solarxai-unified-consumer-db',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': 'true'
        }
    )
    
    env.add_source(kafka_consumer) \
       .map(UnifiedWriteFunction()) \
       .print()
    
    env.execute("UnifiedKafkaConsumer")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] Job failed: {e}")
        raise
