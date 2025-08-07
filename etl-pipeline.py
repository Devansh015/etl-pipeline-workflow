import pandas as pd 
import sqlite3
import requests
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
import json

load_dotenv()

class WeatherETL:
    def __init__(self):
        self.api_key = os.getenv('OPENWEATHER_API_KEY')
        self.city = os.getenv('WEATHER_CITY', 'New York')
        self.country_code = os.getenv('WEATHER_COUNTRY_CODE', 'US')
        self.database_path = os.getenv('DATABASE_PATH', 'data/weather_data.db')
        self.base_url = "http://api.openweathermap.org/data/2.5"
        
    def extract_current_weather(self):
        if not self.api_key:
            raise ValueError("OpenWeatherMap API key not found. Please set OPENWEATHER_API_KEY in your .env file")
        
        url = f"{self.base_url}/weather"
        params = {
            'q': f"{self.city},{self.country_code}",
            'appid': self.api_key,
            'units': 'metric'
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            weather_data = response.json()
            
            print(f"Successfully extracted weather data for {self.city}")
            return weather_data
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching weather data: {e}")
            return None
    
    def extract_forecast_data(self, days=5):
        if not self.api_key:
            raise ValueError("OpenWeatherMap API key not found. Please set OPENWEATHER_API_KEY in your .env file")
        
        url = f"{self.base_url}/forecast"
        params = {
            'q': f"{self.city},{self.country_code}",
            'appid': self.api_key,
            'units': 'metric'
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            forecast_data = response.json()
            
            print(f"Successfully extracted {days}-day forecast data for {self.city}")
            return forecast_data
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching forecast data: {e}")
            return None

    def transform_current_weather(self, weather_data):
        if not weather_data:
            return None
            
        try:
            transformed_data = {
                'timestamp': datetime.now().isoformat(),
                'city': weather_data['name'],
                'country': weather_data['sys']['country'],
                'temperature': weather_data['main']['temp'],
                'feels_like': weather_data['main']['feels_like'],
                'humidity': weather_data['main']['humidity'],
                'pressure': weather_data['main']['pressure'],
                'weather_main': weather_data['weather'][0]['main'],
                'weather_description': weather_data['weather'][0]['description'],
                'wind_speed': weather_data['wind'].get('speed', 0),
                'wind_direction': weather_data['wind'].get('deg', 0),
                'cloudiness': weather_data['clouds']['all'],
                'visibility': weather_data.get('visibility', 0) / 1000,
                'sunrise': datetime.fromtimestamp(weather_data['sys']['sunrise']).isoformat(),
                'sunset': datetime.fromtimestamp(weather_data['sys']['sunset']).isoformat()
            }
            
            df = pd.DataFrame([transformed_data])
            print("Weather data transformed successfully")
            return df
            
        except KeyError as e:
            print(f"Error transforming weather data - missing key: {e}")
            return None

    def transform_forecast_data(self, forecast_data):
        if not forecast_data:
            return None
            
        try:
            forecast_list = []
            
            for item in forecast_data['list']:
                forecast_item = {
                    'forecast_timestamp': datetime.fromtimestamp(item['dt']).isoformat(),
                    'city': forecast_data['city']['name'],
                    'country': forecast_data['city']['country'],
                    'temperature': item['main']['temp'],
                    'feels_like': item['main']['feels_like'],
                    'humidity': item['main']['humidity'],
                    'pressure': item['main']['pressure'],
                    'weather_main': item['weather'][0]['main'],
                    'weather_description': item['weather'][0]['description'],
                    'wind_speed': item['wind'].get('speed', 0),
                    'wind_direction': item['wind'].get('deg', 0),
                    'cloudiness': item['clouds']['all'],
                    'visibility': item.get('visibility', 0) / 1000,
                    'precipitation_probability': item['pop'] * 100
                }
                forecast_list.append(forecast_item)
            
            df = pd.DataFrame(forecast_list)
            print(f"Forecast data transformed successfully - {len(forecast_list)} records")
            return df
            
        except KeyError as e:
            print(f"Error transforming forecast data - missing key: {e}")
            return None

    def load_weather_data(self, df, table_name):
        if df is None or df.empty:
            print("No data to load")
            return
            
        try:
            os.makedirs(os.path.dirname(self.database_path), exist_ok=True)
            
            conn = sqlite3.connect(self.database_path)
            
            df.to_sql(table_name, conn, if_exists='append', index=False)
            
            print(f"Successfully loaded {len(df)} records into {table_name} table")
            conn.close()
            
        except Exception as e:
            print(f"Error loading data to database: {e}")

    def create_weather_tables(self):
        try:
            conn = sqlite3.connect(self.database_path)
            cursor = conn.cursor()
            
            # Create current weather table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS current_weather (
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
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS weather_forecast (
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
            
            conn.commit()
            conn.close()
            print("Weather database tables created successfully")
            
        except Exception as e:
            print(f"Error creating database tables: {e}")

    def run_weather_etl_pipeline(self, include_forecast=True):
        try:
            print("=== Starting Weather ETL Pipeline ===")
            
            self.create_weather_tables()
            
            print("\n1. Processing current weather data...")
            current_weather_raw = self.extract_current_weather()
            if current_weather_raw:
                current_weather_df = self.transform_current_weather(current_weather_raw)
                if current_weather_df is not None:
                    self.load_weather_data(current_weather_df, 'current_weather')
            
            if include_forecast:
                print("\n2. Processing forecast data...")
                forecast_raw = self.extract_forecast_data()
                if forecast_raw:
                    forecast_df = self.transform_forecast_data(forecast_raw)
                    if forecast_df is not None:
                        self.load_weather_data(forecast_df, 'weather_forecast')
            
            print("\n=== Weather ETL Pipeline Completed Successfully ===")
            
        except Exception as e:
            print(f"Error in weather ETL pipeline: {e}")

    def get_latest_weather_summary(self):
        try:
            conn = sqlite3.connect(self.database_path)
            
            current_query = """
                SELECT * FROM current_weather 
                ORDER BY timestamp DESC 
                LIMIT 1
            """
            current_df = pd.read_sql_query(current_query, conn)
            
            today = datetime.now().strftime('%Y-%m-%d')
            forecast_query = """
                SELECT * FROM weather_forecast 
                WHERE DATE(forecast_timestamp) = ? 
                ORDER BY forecast_timestamp
            """
            forecast_df = pd.read_sql_query(forecast_query, conn, params=[today])
            
            conn.close()
            
            print("\n=== Latest Weather Summary ===")
            if not current_df.empty:
                latest = current_df.iloc[0]
                print(f"City: {latest['city']}, {latest['country']}")
                print(f"Temperature: {latest['temperature']}°C (feels like {latest['feels_like']}°C)")
                print(f"Weather: {latest['weather_main']} - {latest['weather_description']}")
                print(f"Humidity: {latest['humidity']}%")
                print(f"Wind: {latest['wind_speed']} m/s")
                
            if not forecast_df.empty:
                print(f"\nToday's Forecast ({len(forecast_df)} data points):")
                print(forecast_df[['forecast_timestamp', 'temperature', 'weather_description']].to_string(index=False))
            
        except Exception as e:
            print(f"Error getting weather summary: {e}")


def extract_data(file_path):
    data = pd.read_csv(file_path)
    return data

def transform_data(data):
    data = data.dropna()  
    data = data[data['0'] > 18]
    return data

def load_data(data, database_path):
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            name TEXT,
            age INTEGER
        )
    ''')        

    for _, row in data.iterrows():
        cursor.execute('''
            INSERT INTO users (name, age) VALUES (?, ?)
        ''', (row['1'], row['0']))

    conn.commit()   
    conn.close()

def run_legacy_etl_pipeline():
    try:
        data = extract_data('data/source_data.csv')
        transformed_data = transform_data(data)
        load_data(transformed_data, 'data/destination.db')
        print("Legacy ETL pipeline completed successfully")
        
    except Exception as e:
        print(f"Error occurred in legacy pipeline: {e}")


if __name__ == "__main__":
    print("Choose ETL pipeline to run:")
    print("1. Weather ETL Pipeline (recommended)")
    print("2. Legacy CSV ETL Pipeline")
    print("3. Both pipelines")
    print("4. Weather summary only")
    
    choice = input("Enter your choice (1-4): ").strip()
    
    if choice in ['1', '3']:
        weather_etl = WeatherETL()
        weather_etl.run_weather_etl_pipeline()
        weather_etl.get_latest_weather_summary()
    
    if choice in ['2', '3']:
        if os.path.exists('data/source_data.csv'):
            run_legacy_etl_pipeline()
        else:
            print("CSV file not found. Skipping legacy pipeline.")
    
    if choice == '4':
        weather_etl = WeatherETL()
        weather_etl.get_latest_weather_summary()
