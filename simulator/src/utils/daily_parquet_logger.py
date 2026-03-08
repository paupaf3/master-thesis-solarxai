import pandas as pd
from datetime import datetime
from pathlib import Path

SAVE_DATA_TESTING = False  # Enable data saving for testing
class DailyParquetLogger:
    
    def __init__(self, output_dir: str):
        if not SAVE_DATA_TESTING:
            return
        
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
      
        self.current_day = None
        self.daily_buffer = []


    def add_record(self, ts: datetime, data: dict):
        if not SAVE_DATA_TESTING:
            return
            
        day_str = ts.strftime("%Y-%m-%d")

        # Si cambia el día, guardar lo anterior
        if self.current_day and day_str != self.current_day:
            self._flush_to_parquet()

        self.current_day = day_str
        record = data.copy()
        record["timestamp"] = ts
        self.daily_buffer.append(record)


    def _flush_to_parquet(self):
        if not SAVE_DATA_TESTING:
            return
        
        if not self.daily_buffer or not self.current_day:
            return

        df = pd.DataFrame(self.daily_buffer)
        file_path = self.output_dir / f"{self.current_day}.parquet"
        df.to_parquet(file_path, index=False, engine="pyarrow", compression="snappy")

        self.daily_buffer.clear()


    def flush_all(self):
        if not SAVE_DATA_TESTING:
            return
        
        self._flush_to_parquet()
