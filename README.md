# ETL Pipeline with Daily Weather Data

## Overview

This project implements a robust ETL (Extract, Transform, Load) pipeline designed to automate data ingestion, cleaning, transformation, and storage. The pipeline now includes **daily weather data collection** from the OpenWeatherMap API, alongside support for traditional CSV data processing.

## Features

### Weather Data Pipeline
- **Extract** current weather data and forecasts from OpenWeatherMap API
- **Transform** weather data into structured format with comprehensive metrics
- **Load** data into SQLite database with proper schema
- **Schedule** daily data collection
- **Monitor** weather trends and historical data

### Traditional ETL
- Extract data from CSV files and other sources
- Transform data using pandas with custom transformation logic
- Load data into SQLite databases
- Modular, extensible architecture

### General Features
- Comprehensive logging and error handling
- Environment-based configuration
- Backward compatibility with existing pipelines
- Database schema management

## Tech Stack

- **Python 3.x**
- **pandas** - Data manipulation and analysis
- **requests** - HTTP requests for API calls
- **sqlite3** - Database storage
- **python-dotenv** - Environment variable management

## Quick Start

### 1. Installation

```bash
# Install required packages
pip install -r requirements.txt
```

### 2. API Setup

1. Get a free API key from [OpenWeatherMap](https://openweathermap.org/api)
2. Copy the configuration file:
   ```bash
   cp .env.example .env
   ```
3. Edit `.env` and add your API key:
   ```
   OPENWEATHER_API_KEY=your_actual_api_key_here
   WEATHER_CITY=Your City
   WEATHER_COUNTRY_CODE=US
   DATABASE_PATH=data/weather_data.db
   ```

### 3. Run the Pipeline

```bash
# Run the weather ETL pipeline
python etl-pipeline.py
```

Choose option 1 for the weather pipeline when prompted.

## Usage Examples

### Weather Data Collection

```python
from etl-pipeline import WeatherETL

# Initialize the weather ETL
weather_etl = WeatherETL()

# Run complete pipeline (current weather + forecast)
weather_etl.run_weather_etl_pipeline()

# Get latest weather summary
weather_etl.get_latest_weather_summary()
```

### Daily Scheduling

For automated daily data collection:

```bash
# Run the daily scheduler
python daily_weather_scheduler.py
```

Set up as a cron job for automatic execution:
```bash
# Add to crontab (runs daily at 8 AM)
0 8 * * * cd /path/to/etl-pipeline && python daily_weather_scheduler.py
```

## Database Schema

### Current Weather Table
- `timestamp` - When the data was collected
- `city`, `country` - Location information
- `temperature`, `feels_like` - Temperature data (°C)
- `humidity`, `pressure` - Atmospheric conditions
- `weather_main`, `weather_description` - Weather conditions
- `wind_speed`, `wind_direction` - Wind data
- `cloudiness`, `visibility` - Visibility conditions
- `sunrise`, `sunset` - Sun times

### Weather Forecast Table
- Similar to current weather with additional:
- `forecast_timestamp` - Future time prediction
- `precipitation_probability` - Chance of rain (%)

## Architecture

```
ETL Pipeline Architecture:

1. EXTRACT
   ├── OpenWeatherMap API (Current Weather)
   ├── OpenWeatherMap API (5-day Forecast)
   └── CSV Files (Legacy)

2. TRANSFORM
   ├── Data Cleaning & Validation
   ├── Unit Conversions
   ├── Schema Standardization
   └── Error Handling

3. LOAD
   ├── SQLite Database
   ├── Structured Tables
   └── Data Integrity Checks
```

## Data Flow

1. **Extract**: Fetch weather data from API endpoints
2. **Transform**: Convert raw JSON to structured DataFrame
3. **Load**: Insert processed data into SQLite tables
4. **Monitor**: Log activities and generate summaries

## Error Handling

The pipeline includes comprehensive error handling:
- API connection failures
- Data validation errors
- Database connection issues
- Missing configuration values

## Logging

Logs are stored in the `logs/` directory with daily rotation:
- `logs/weather_etl_YYYYMMDD.log`

## Legacy Support

The pipeline maintains backward compatibility with existing CSV-based ETL processes. You can run both weather and CSV pipelines simultaneously.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Submit a pull request

