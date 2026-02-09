import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
import time
from datetime import date, timedelta
from pathlib import Path

# --- Configuration ---
CACHE_DIR = ".cache"
OUTPUT_DIR = "data/raw"
OUTPUT_FILENAME = "weather.csv"
START_DATE = "2014-01-01"

# API Parameters
URL = "https://archive-api.open-meteo.com/v1/archive"
METRICS = ["temperature_2m_max", "temperature_2m_min", "rain_sum", "shortwave_radiation_sum"]

# Coordinates for Pakistan (North to South)
LOCATIONS = {
    "Rawalpindi": {"lat": 33.6007, "lon": 73.0679},
    "Islamabad":  {"lat": 33.6844, "lon": 73.0479},
    "Abbottabad": {"lat": 34.1688, "lon": 73.2215},
    "Gilgit":     {"lat": 35.9208, "lon": 74.3089},
    "Skardu":     {"lat": 35.2951, "lon": 75.6337},
    "Lahore":     {"lat": 31.5497, "lon": 74.3436},
    "Faisalabad": {"lat": 31.4504, "lon": 73.1350},
    "Sialkot":    {"lat": 32.4945, "lon": 74.5229},
    "Multan":     {"lat": 30.1575, "lon": 71.5249},
    "Bahawalpur": {"lat": 29.3544, "lon": 71.6911},
    "Karachi":    {"lat": 24.8607, "lon": 67.0011},
    "Hyderabad":  {"lat": 25.3960, "lon": 68.3578},
    "Sukkur":     {"lat": 27.7131, "lon": 68.8524},
    "Quetta":     {"lat": 30.1798, "lon": 66.9750},
    "Gwadar":     {"lat": 25.1216, "lon": 62.3254}
}

def get_openmeteo_client():
    """Sets up the API client with caching and retry logic."""
    cache_session = requests_cache.CachedSession(CACHE_DIR, expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    return openmeteo_requests.Client(session=retry_session)

def process_response(response, city_name):
    """Parses the binary response from Open-Meteo into a DataFrame."""
    daily = response.Daily()
    
    # Generate date range
    start = pd.to_datetime(daily.Time(), unit="s", utc=True)
    end = pd.to_datetime(daily.TimeEnd(), unit="s", utc=True)
    interval = pd.Timedelta(seconds=daily.Interval())
    
    dates = pd.date_range(start=start, end=end, freq=interval, inclusive="left")
    
    # Extract variables dynamically based on index
    data = {"date": dates, "city": city_name}
    
    # Note: The order here must match the 'daily' list in params
    data["temp_max"] = daily.Variables(0).ValuesAsNumpy()
    data["temp_min"] = daily.Variables(1).ValuesAsNumpy()
    data["rain"] = daily.Variables(2).ValuesAsNumpy()
    data["solar_radiation"] = daily.Variables(3).ValuesAsNumpy()

    return pd.DataFrame(data)

def fetch_weather_data():
    client = get_openmeteo_client()
    all_data = []

    # Calculate dynamic end date (Yesterday to ensure data availability)
    today = date.today()
    end_date_str = (today - timedelta(days=2)).strftime("%Y-%m-%d")

    print(f"Starting ingestion. Period: {START_DATE} to {end_date_str}")
    print(f"Targeting {len(LOCATIONS)} cities...")

    for city, coords in LOCATIONS.items():
        print(f"Fetching: {city}...", end=" ", flush=True)
        
        params = {
            "latitude": coords["lat"],
            "longitude": coords["lon"],
            "start_date": START_DATE,
            "end_date": end_date_str,
            "daily": METRICS,
            "timezone": "auto"
        }

        attempts = 0
        max_attempts = 3
        
        while attempts < max_attempts:
            try:
                responses = client.weather_api(URL, params=params)
                df = process_response(responses[0], city)
                all_data.append(df)
                print("Done.")
                time.sleep(2) # Politeness delay
                break
            
            except Exception as e:
                attempts += 1
                error_msg = str(e).lower()
                
                if "429" in error_msg or "limit" in error_msg:
                    print(f"\nRate limit hit. Pausing 60s (Attempt {attempts}/{max_attempts})...")
                    time.sleep(60)
                else:
                    print(f"\nError fetching {city}: {e}")
                    break

    return pd.concat(all_data) if all_data else pd.DataFrame()

def save_data(df):
    if df.empty:
        print("No data collected.")
        return

    output_path = Path(OUTPUT_DIR)
    output_path.mkdir(parents=True, exist_ok=True)
    file_path = output_path / OUTPUT_FILENAME

    df.to_csv(file_path, index=False)
    
    print("-" * 30)
    print(f"Ingestion Complete.")
    print(f"Rows collected: {len(df)}")
    print(f"Saved to: {file_path}")

if __name__ == "__main__":
    weather_df = fetch_weather_data()
    save_data(weather_df)