#!/usr/bin/env python3

import pandas as pd
import sqlite3
import os
from datetime import datetime
import json

def create_sample_weather_data():
    sample_current = {
        'timestamp': datetime.now().isoformat(),
        'city': 'Demo City',
        'country': 'US',
        'temperature': 22.5,
        'feels_like': 24.1,
        'humidity': 65,
        'pressure': 1013.25,
        'weather_main': 'Clear',
        'weather_description': 'clear sky',
        'wind_speed': 3.2,
        'wind_direction': 180,
        'cloudiness': 10,
        'visibility': 10.0,
        'sunrise': '2025-08-07T06:30:00',
        'sunset': '2025-08-07T19:45:00'
    }
    
    # Create sample forecast data (5 data points)
    sample_forecast = []
    base_temp = 20.0
    for i in range(5):
        forecast_item = {
            'forecast_timestamp': datetime.now().replace(hour=6+i*3).isoformat(),
            'city': 'Demo City',
            'country': 'US',
            'temperature': base_temp + i * 2,
            'feels_like': base_temp + i * 2 + 1.5,
            'humidity': 60 + i * 5,
            'pressure': 1010 + i,
            'weather_main': ['Clear', 'Clouds', 'Rain', 'Clear', 'Clouds'][i],
            'weather_description': ['clear sky', 'few clouds', 'light rain', 'clear sky', 'scattered clouds'][i],
            'wind_speed': 2.0 + i * 0.5,
            'wind_direction': 180 + i * 30,
            'cloudiness': 10 + i * 20,
            'visibility': 10.0 - i * 0.5,
            'precipitation_probability': i * 20
        }
        sample_forecast.append(forecast_item)
    
    return sample_current, sample_forecast

def demo_etl_pipeline():
    print("=== Weather ETL Demo Pipeline ===")
    
    current_data, forecast_data = create_sample_weather_data()
    
    current_df = pd.DataFrame([current_data])
    forecast_df = pd.DataFrame(forecast_data)
    
    print(f"\n1. EXTRACT - Sample weather data created")
    print(f"   Current weather: 1 record")
    print(f"   Forecast data: {len(forecast_data)} records")
    
    print(f"\n2. TRANSFORM - Data structure:")
    print("\nCurrent Weather Sample:")
    print(current_df[['city', 'temperature', 'weather_description', 'humidity']].to_string(index=False))
    
    print("\nForecast Sample (first 3 records):")
    print(forecast_df[['forecast_timestamp', 'temperature', 'weather_description']][:3].to_string(index=False))
    
    db_path = 'data/demo_weather.db'
    os.makedirs('data', exist_ok=True)
    
    try:
        conn = sqlite3.connect(db_path)
        
        conn.execute('''
            CREATE TABLE IF NOT EXISTS current_weather_demo (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                city TEXT,
                country TEXT,
                temperature REAL,
                feels_like REAL,
                humidity INTEGER,
                pressure REAL,
                weather_main TEXT,
                weather_description TEXT,
                wind_speed REAL,
                wind_direction REAL,
                cloudiness INTEGER,
                visibility REAL,
                sunrise TEXT,
                sunset TEXT
            )
        ''')
        
        conn.execute('''
            CREATE TABLE IF NOT EXISTS weather_forecast_demo (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                forecast_timestamp TEXT,
                city TEXT,
                country TEXT,
                temperature REAL,
                feels_like REAL,
                humidity INTEGER,
                pressure REAL,
                weather_main TEXT,
                weather_description TEXT,
                wind_speed REAL,
                wind_direction REAL,
                cloudiness INTEGER,
                visibility REAL,
                precipitation_probability REAL
            )
        ''')
        
        current_df.to_sql('current_weather_demo', conn, if_exists='append', index=False)
        forecast_df.to_sql('weather_forecast_demo', conn, if_exists='append', index=False)
        
        print(f"\n3. LOAD - Data successfully loaded to {db_path}")
        print(f"   Tables created: current_weather_demo, weather_forecast_demo")
        
        current_count = conn.execute("SELECT COUNT(*) FROM current_weather_demo").fetchone()[0]
        forecast_count = conn.execute("SELECT COUNT(*) FROM weather_forecast_demo").fetchone()[0]
        
        print(f"   Records in current_weather_demo: {current_count}")
        print(f"   Records in weather_forecast_demo: {forecast_count}")
        
        conn.close()
        
        print("\n=== Demo Pipeline Completed Successfully ===")
        print(f"\nNext Steps:")
        print(f"1. Get an API key from https://openweathermap.org/api")
        print(f"2. Update your .env file with the API key")
        print(f"3. Run the full pipeline: python etl-pipeline.py")
        
    except Exception as e:
        print(f"Error in demo pipeline: {e}")

def show_database_contents():
    db_path = 'data/demo_weather.db'
    
    if not os.path.exists(db_path):
        print("Demo database not found. Run the demo pipeline first.")
        return
    
    try:
        conn = sqlite3.connect(db_path)
        
        print("\n=== Demo Database Contents ===")
        
        current_df = pd.read_sql_query("SELECT * FROM current_weather_demo ORDER BY timestamp DESC LIMIT 5", conn)
        if not current_df.empty:
            print("\nCurrent Weather Records:")
            print(current_df[['timestamp', 'city', 'temperature', 'weather_description']].to_string(index=False))
        
        forecast_df = pd.read_sql_query("SELECT * FROM weather_forecast_demo ORDER BY forecast_timestamp LIMIT 5", conn)
        if not forecast_df.empty:
            print("\nForecast Records:")
            print(forecast_df[['forecast_timestamp', 'temperature', 'weather_description']].to_string(index=False))
        
        conn.close()
        
    except Exception as e:
        print(f"Error reading database: {e}")

if __name__ == "__main__":
    print("Weather ETL Demo")
    print("1. Run demo pipeline")
    print("2. Show database contents")
    print("3. Exit")
    
    choice = input("Enter your choice (1-3): ").strip()
    
    if choice == "1":
        demo_etl_pipeline()
    elif choice == "2":
        show_database_contents()
    elif choice == "3":
        print("Goodbye!")
    else:
        print("Invalid choice. Running demo pipeline...")
        demo_etl_pipeline()
