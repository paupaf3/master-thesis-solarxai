"""
Unified Silver to Gold Aggregation for SolarX.ai
Continuously aggregates ALL data types from silver layer to gold layer
(hourly and daily aggregations).
Single job with higher parallelism for better memory efficiency.
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import FlatMapFunction, RuntimeContext
from pyflink.common.typeinfo import Types
from datetime import datetime, timedelta
import time
import sys
import traceback
sys.path.append('/opt/flink/jobs')
from utils.db_utils import DatabaseConfig, GoldLayerWriter


class UnifiedSilverToGoldFunction(FlatMapFunction):
    """
    Continuously aggregates all data types from silver to gold layer.
    - Processes hourly aggregations for the previous hour
    - Processes daily aggregations at the end of day
    - Handles inverter, meteo, poi_meter, and system_status data
    """
    
    def __init__(self, poll_interval_seconds: int = 60):
        self.writer = None
        self.poll_interval_seconds = poll_interval_seconds
        self.running = True
        self.last_hourly_run = None
        self.last_daily_run = None
    
    def open(self, runtime_context: RuntimeContext):
        """Initialize database writer"""
        db_config = DatabaseConfig()
        self.writer = GoldLayerWriter(db_config)
        print(f"[UNIFIED S2G] Initialized GoldLayerWriter")
    
    def flat_map(self, value):
        """
        Continuously process silver data into gold aggregations for all data types.
        """
        print(f"[UNIFIED S2G] Starting continuous aggregation loop...")
        
        while self.running:
            try:
                current_time = datetime.now()
                
                # Process hourly aggregations
                if self._should_run_hourly(current_time):
                    yield from self._process_all_hourly(current_time)
                    self.last_hourly_run = current_time
                
                # Process daily aggregations
                if self._should_run_daily(current_time):
                    yield from self._process_all_daily(current_time)
                    self.last_daily_run = current_time
                
            except Exception as e:
                error_output = f"[UNIFIED S2G] {datetime.now().isoformat()} | Error: {e}"
                print(error_output)
                yield error_output
            
            time.sleep(self.poll_interval_seconds)
    
    def _should_run_hourly(self, current_time: datetime) -> bool:
        """Check if hourly aggregation should run (every 5 minutes for DEBUG)."""
        if self.last_hourly_run is None:
            return True
        # TODO DEBUG
        return (current_time - self.last_hourly_run) >= timedelta(minutes=1)
        # return current_time.hour != self.last_hourly_run.hour
    
    def _should_run_daily(self, current_time: datetime) -> bool:
        """Check if daily aggregation should run."""
        if self.last_daily_run is None:
            return True
        # TODO DEBUG
        return (current_time - self.last_daily_run) >= timedelta(minutes=2)
        # return current_time.date() != self.last_daily_run.date()
    
    def _process_all_hourly(self, current_time: datetime):
        """Process hourly aggregations for all data types based on unaggregated silver data."""
        # Inverter hourly
        inverter_hours = self.writer.get_unaggregated_inverter_hours()
        inv_count = 0
        for plant_id, inverter_id, hour in inverter_hours:
            try:
                result = self.writer.aggregate_inverter_hourly(str(plant_id), inverter_id, hour.isoformat())
                if result:
                    inv_count += 1
            except Exception as e:
                tb = traceback.format_exc()
                yield f"[INVERTER S2G] Error hourly {inverter_id}: {e}\n{tb}"
        if inv_count > 0:
            yield f"[INVERTER S2G] {datetime.now().isoformat()} | Hourly: {inv_count} records"
        
        # Meteo hourly
        meteo_hours = self.writer.get_unaggregated_meteo_hours()
        meteo_count = 0
        for plant_id, hour in meteo_hours:
            try:
                result = self.writer.aggregate_meteo_hourly(str(plant_id), hour.isoformat())
                if result:
                    meteo_count += 1
            except Exception as e:
                yield f"[METEO S2G] Error hourly plant {plant_id}: {e}"
        if meteo_count > 0:
            yield f"[METEO S2G] {datetime.now().isoformat()} | Hourly: {meteo_count} records"
        
        # POI meter hourly
        poi_hours = self.writer.get_unaggregated_poi_meter_hours()
        poi_count = 0
        for plant_id, hour in poi_hours:
            try:
                result = self.writer.aggregate_poi_meter_hourly(str(plant_id), hour.isoformat())
                if result:
                    poi_count += 1
            except Exception as e:
                yield f"[POI_METER S2G] Error hourly plant {plant_id}: {e}"
        if poi_count > 0:
            yield f"[POI_METER S2G] {datetime.now().isoformat()} | Hourly: {poi_count} records"
        
        # System status hourly
        status_hours = self.writer.get_unaggregated_system_status_hours()
        status_count = 0
        for plant_id, hour in status_hours:
            try:
                result = self.writer.aggregate_system_status_hourly(str(plant_id), hour.isoformat())
                if result:
                    status_count += 1
            except Exception as e:
                yield f"[SYSTEM_STATUS S2G] Error hourly plant {plant_id}: {e}"
        if status_count > 0:
            yield f"[SYSTEM_STATUS S2G] {datetime.now().isoformat()} | Hourly: {status_count} records"
        
        # Plant summary hourly (after inverter hourly is done)
        summary_hours = self.writer.get_unaggregated_plant_summary_hours()
        summary_count = 0
        for plant_id, hour in summary_hours:
            try:
                result = self.writer.aggregate_plant_hourly_summary(str(plant_id), hour.isoformat())
                if result:
                    summary_count += 1
            except Exception as e:
                yield f"[PLANT_SUMMARY S2G] Error hourly plant {plant_id}: {e}"
        if summary_count > 0:
            yield f"[PLANT_SUMMARY S2G] {datetime.now().isoformat()} | Hourly: {summary_count} records"
    
    def _process_all_daily(self, current_time: datetime):
        """Process daily aggregations for all data types based on unaggregated gold hourly data."""
        # Inverter daily
        inverter_dates = self.writer.get_unaggregated_inverter_dates()
        inv_count = 0
        for plant_id, inverter_id, day in inverter_dates:
            try:
                result = self.writer.aggregate_inverter_daily(str(plant_id), inverter_id, day.isoformat())
                if result:
                    inv_count += 1
            except Exception as e:
                yield f"[INVERTER S2G] Error daily {inverter_id}: {e}"
        if inv_count > 0:
            yield f"[INVERTER S2G] {datetime.now().isoformat()} | Daily: {inv_count} records"
        
        # Meteo daily
        meteo_dates = self.writer.get_unaggregated_meteo_dates()
        meteo_count = 0
        for plant_id, day in meteo_dates:
            try:
                result = self.writer.aggregate_meteo_daily(str(plant_id), day.isoformat())
                if result:
                    meteo_count += 1
            except Exception as e:
                yield f"[METEO S2G] Error daily plant {plant_id}: {e}"
        if meteo_count > 0:
            yield f"[METEO S2G] {datetime.now().isoformat()} | Daily: {meteo_count} records"
        
        # POI meter daily
        poi_dates = self.writer.get_unaggregated_poi_meter_dates()
        poi_count = 0
        for plant_id, day in poi_dates:
            try:
                result = self.writer.aggregate_poi_meter_daily(str(plant_id), day.isoformat())
                if result:
                    poi_count += 1
            except Exception as e:
                yield f"[POI_METER S2G] Error daily plant {plant_id}: {e}"
        if poi_count > 0:
            yield f"[POI_METER S2G] {datetime.now().isoformat()} | Daily: {poi_count} records"
        
        # System status daily
        status_dates = self.writer.get_unaggregated_system_status_dates()
        status_count = 0
        for plant_id, day in status_dates:
            try:
                result = self.writer.aggregate_system_status_daily(str(plant_id), day.isoformat())
                if result:
                    status_count += 1
            except Exception as e:
                yield f"[SYSTEM_STATUS S2G] Error daily plant {plant_id}: {e}"
        if status_count > 0:
            yield f"[SYSTEM_STATUS S2G] {datetime.now().isoformat()} | Daily: {status_count} records"
        
        # Plant summary daily
        summary_dates = self.writer.get_unaggregated_plant_summary_dates()
        summary_count = 0
        for plant_id, day in summary_dates:
            try:
                result = self.writer.aggregate_plant_daily_summary(str(plant_id), day.isoformat())
                if result:
                    summary_count += 1
            except Exception as e:
                yield f"[PLANT_SUMMARY S2G] Error daily plant {plant_id}: {e}"
        if summary_count > 0:
            yield f"[PLANT_SUMMARY S2G] {datetime.now().isoformat()} | Daily: {summary_count} records"


def main():
    """Main entry point for unified silver-to-gold aggregation"""
    
    POLL_INTERVAL_SECONDS = 60  # Check every minute
    
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(3)  # Moderate parallelism for aggregation job
    
    print("[INFO] Starting Unified Silver-to-Gold Aggregation")
    print(f"[INFO] Poll Interval: {POLL_INTERVAL_SECONDS}s")
    print("[INFO] Processing: inverter, meteo, poi_meter, system_status, plant_summary")
    print("[INFO] " + "="*60)
    
    trigger_stream = env.from_collection(
        collection=["start_unified_s2g"],
        type_info=Types.STRING()
    )
    
    result_stream = trigger_stream.flat_map(
        UnifiedSilverToGoldFunction(poll_interval_seconds=POLL_INTERVAL_SECONDS),
        output_type=Types.STRING()
    )
    
    result_stream.print()
    
    env.execute("UnifiedSilverToGold")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] Job failed: {e}")
        raise
