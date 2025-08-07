#!/usr/bin/env python3

import sys
import os
from datetime import datetime
import logging

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    import importlib.util
    spec = importlib.util.spec_from_file_location("etl_pipeline", "etl-pipeline.py")
    etl_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(etl_module)
    WeatherETL = etl_module.WeatherETL
except ImportError as e:
    print(f"Error importing WeatherETL: {e}")
    sys.exit(1)

def setup_logging():
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    
    log_filename = f"{log_dir}/weather_etl_{datetime.now().strftime('%Y%m%d')}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    return logging.getLogger(__name__)

def run_daily_weather_collection():
    logger = setup_logging()
    
    try:
        logger.info("Starting daily weather data collection")
        
        weather_etl = WeatherETL()
        
        weather_etl.run_weather_etl_pipeline(include_forecast=True)
        
        logger.info("Daily weather data collection completed successfully")
        
        weather_etl.get_latest_weather_summary()
        
        return True
        
    except Exception as e:
        logger.error(f"Error in daily weather collection: {e}")
        return False

def main():
    print(f"=== Daily Weather ETL Scheduler - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===")
    
    success = run_daily_weather_collection()
    
    if success:
        print("Daily weather data collection completed successfully!")
        sys.exit(0)
    else:
        print("Daily weather data collection failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
