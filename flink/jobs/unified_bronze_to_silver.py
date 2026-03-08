"""
Unified Bronze to Silver Transformation for SolarX.ai
Continuously processes ALL data types from bronze layer, validates,
cleans, and stores in silver layer.
Single job with higher parallelism for better memory efficiency.
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import FlatMapFunction, RuntimeContext
from pyflink.common.typeinfo import Types
from datetime import datetime
import json
import os
import time
import sys
sys.path.append('/opt/flink/jobs')
from utils.db_utils import DatabaseConfig, SilverLayerWriter


class UnifiedBronzeToSilverFunction(FlatMapFunction):
    """
    Continuously processes all data types from bronze layer to silver layer.
    - Runs a polling loop to fetch unprocessed records for each type
    - Validates and cleans data
    - Writes to silver layer
    - Updates bronze processing status
    """
    
    KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "broker-1:29092,broker-2:29192,broker-3:29292")
    TRIGGER_TOPIC = "inference-trigger"

    def __init__(self, batch_size: int = 100, poll_interval_seconds: int = 5):
        self.writer = None
        self.producer = None
        self.batch_size = batch_size
        self.poll_interval_seconds = poll_interval_seconds
        self.running = True
    
    def open(self, runtime_context: RuntimeContext):
        """Initialize database writer and Kafka producer"""
        db_config = DatabaseConfig()
        self.writer = SilverLayerWriter(db_config)
        print(f"[UNIFIED B2S] Initialized SilverLayerWriter")
        try:
            from kafka import KafkaProducer
            self.producer = KafkaProducer(
                bootstrap_servers=self.KAFKA_BROKERS.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            print(f"[UNIFIED B2S] Initialized KafkaProducer for {self.TRIGGER_TOPIC}")
        except Exception as e:
            print(f"[UNIFIED B2S] WARNING: Could not init KafkaProducer: {e}")
            self.producer = None
    
    def flat_map(self, value):
        """
        Continuously fetch and process bronze records for all data types.
        """
        print(f"[UNIFIED B2S] Starting continuous processing loop...")
        
        while self.running:
            try:
                # Process each data type
                yield from self._process_inverter_records()
                yield from self._process_meteo_records()
                yield from self._process_poi_meter_records()
                yield from self._process_system_status_records()
                
            except Exception as e:
                error_output = f"[UNIFIED B2S] {datetime.now().isoformat()} | Loop Error: {e}"
                print(error_output)
                yield error_output
            
            time.sleep(self.poll_interval_seconds)
    
    def _process_inverter_records(self):
        """Process inverter records from bronze to silver"""
        try:
            records = self.writer.get_unprocessed_inverter_records(self.batch_size)
            
            if not records:
                return
            
            processed_count = 0
            failed_count = 0
            
            for record in records:
                try:
                    silver_id = self.writer.insert_silver_inverter(record)
                    if silver_id:
                        processed_count += 1
                except Exception as e:
                    failed_count += 1
                    yield f"[INVERTER B2S] Error processing Bronze ID {record.get('id')}: {e}"
            
            if processed_count > 0 or failed_count > 0:
                summary = f"[INVERTER B2S] {datetime.now().isoformat()} | "
                summary += f"Batch: {processed_count} processed, {failed_count} failed"
                print(summary)
                yield summary

            if processed_count > 0:
                self._send_inference_trigger("inverter", processed_count)
                
        except Exception as e:
            yield f"[INVERTER B2S] {datetime.now().isoformat()} | Batch Error: {e}"
    
    def _process_meteo_records(self):
        """Process meteo records from bronze to silver"""
        try:
            records = self.writer.get_unprocessed_meteo_records(self.batch_size)
            
            if not records:
                return
            
            processed_count = 0
            failed_count = 0
            
            for record in records:
                try:
                    silver_id = self.writer.insert_silver_meteo(record)
                    if silver_id:
                        processed_count += 1
                except Exception as e:
                    failed_count += 1
                    yield f"[METEO B2S] Error processing Bronze ID {record.get('id')}: {e}"
            
            if processed_count > 0 or failed_count > 0:
                summary = f"[METEO B2S] {datetime.now().isoformat()} | "
                summary += f"Batch: {processed_count} processed, {failed_count} failed"
                print(summary)
                yield summary

            if processed_count > 0:
                self._send_inference_trigger("meteo", processed_count)
                
        except Exception as e:
            yield f"[METEO B2S] {datetime.now().isoformat()} | Batch Error: {e}"
    
    def _process_poi_meter_records(self):
        """Process POI meter records from bronze to silver"""
        try:
            records = self.writer.get_unprocessed_poi_meter_records(self.batch_size)
            
            if not records:
                return
            
            processed_count = 0
            failed_count = 0
            
            for record in records:
                try:
                    silver_id = self.writer.insert_silver_poi_meter(record)
                    if silver_id:
                        processed_count += 1
                except Exception as e:
                    failed_count += 1
                    yield f"[POI_METER B2S] Error processing Bronze ID {record.get('id')}: {e}"
            
            if processed_count > 0 or failed_count > 0:
                summary = f"[POI_METER B2S] {datetime.now().isoformat()} | "
                summary += f"Batch: {processed_count} processed, {failed_count} failed"
                print(summary)
                yield summary
                
        except Exception as e:
            yield f"[POI_METER B2S] {datetime.now().isoformat()} | Batch Error: {e}"
    
    def _process_system_status_records(self):
        """Process system status records from bronze to silver"""
        try:
            records = self.writer.get_unprocessed_system_status_records(self.batch_size)
            
            if not records:
                return
            
            processed_count = 0
            failed_count = 0
            
            for record in records:
                try:
                    silver_id = self.writer.insert_silver_system_status(record)
                    if silver_id:
                        processed_count += 1
                except Exception as e:
                    failed_count += 1
                    yield f"[SYSTEM_STATUS B2S] Error processing Bronze ID {record.get('id')}: {e}"
            
            if processed_count > 0 or failed_count > 0:
                summary = f"[SYSTEM_STATUS B2S] {datetime.now().isoformat()} | "
                summary += f"Batch: {processed_count} processed, {failed_count} failed"
                print(summary)
                yield summary
                
        except Exception as e:
            yield f"[SYSTEM_STATUS B2S] {datetime.now().isoformat()} | Batch Error: {e}"

    def _send_inference_trigger(self, data_type: str, count: int):
        """Publish a notification to the inference-trigger Kafka topic."""
        if self.producer is None:
            return
        try:
            msg = {
                "event": "silver_batch_processed",
                "data_type": data_type,
                "count": count,
                "timestamp": datetime.now().isoformat(),
            }
            self.producer.send(self.TRIGGER_TOPIC, value=msg)
            self.producer.flush()
        except Exception as e:
            print(f"[UNIFIED B2S] WARNING: Failed to send inference trigger: {e}")


def main():
    """Main entry point for unified bronze-to-silver transformation"""
    
    BATCH_SIZE = 100
    POLL_INTERVAL_SECONDS = 5
    
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(3)  # Higher parallelism for unified job
    
    print("[INFO] Starting Unified Bronze-to-Silver Transformation")
    print(f"[INFO] Batch Size: {BATCH_SIZE}")
    print(f"[INFO] Poll Interval: {POLL_INTERVAL_SECONDS}s")
    print("[INFO] Processing: inverter, meteo, poi_meter, system_status")
    print("[INFO] " + "="*60)
    
    env.from_collection(['start'], type_info=Types.STRING()) \
       .flat_map(UnifiedBronzeToSilverFunction(BATCH_SIZE, POLL_INTERVAL_SECONDS)) \
       .print()
    
    env.execute("UnifiedBronzeToSilver")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] Job failed: {e}")
        raise
