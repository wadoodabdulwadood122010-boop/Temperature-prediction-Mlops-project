import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
import os
import time

# Setup Open-Meteo API Client with cache and retry
cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

# EXPANDED LOCATIONS: Covers North, South, Central, and Coastal Pakistan
LOCATIONS = {
    # Twin Cities & Potohar
    "Rawalpindi": {"lat": 33.6007, "lon": 73.0679},
    "Islamabad":  {"lat": 33.6844, "lon": 73.0479},
    
    # Northern Areas (Critical for cold wave prediction)
    "Abbottabad": {"lat": 34.1688, "lon": 73.2215}, # Added for your specific interest
    "Gilgit":     {"lat": 35.9208, "lon": 74.3089},
    "Skardu":     {"lat": 35.2951, "lon": 75.6337}, # Extreme cold training data
    
    # Central Punjab (Urban Heat Islands)
    "Lahore":     {"lat": 31.5497, "lon": 74.3436},
    "Faisalabad": {"lat": 31.4504, "lon": 73.1350},
    "Sialkot":    {"lat": 32.4945, "lon": 74.5229}, # Humid/Industrial
    
    # South Punjab (Extreme Heat)
    "Multan":     {"lat": 30.1575, "lon": 71.5249},
    "Bahawalpur": {"lat": 29.3544, "lon": 71.6911}, # Desert climate

    # Sindh (Humid & Dry Heat)
    "Karachi":    {"lat": 24.8607, "lon": 67.0011}, # Coastal Humidity
    "Hyderabad":  {"lat": 25.3960, "lon": 68.3578},
    "Sukkur":     {"lat": 27.7131, "lon": 68.8524}, # Record breaking heat area

    # Baluchistan & Coast
    "Quetta":     {"lat": 30.1798, "lon": 66.9750}, # Dry Cold
    "Gwadar":     {"lat": 25.1216, "lon": 62.3254}  # Deep Sea Port/Coastal
}

def fetch_data():
    all_data = []
    
    url = "https://archive-api.open-meteo.com/v1/archive"
    
    for city, coords in LOCATIONS.items():
        print(f"Fetching 10 years of data for {city}...")
        
        params = {
            "latitude": coords["lat"],
            "longitude": coords["lon"],
            "start_date": "2014-01-01",
            "end_date": "2024-01-01",
            "daily": ["temperature_2m_max", "temperature_2m_min", "rain_sum", "shortwave_radiation_sum"],
            "timezone": "auto"
        }
        
        try:
            responses = openmeteo.weather_api(url, params=params)
            response = responses[0]
            
            # Process daily data
            daily = response.Daily()
            daily_data = {
                "date": pd.date_range(
                    start=pd.to_datetime(daily.Time(), unit="s", utc=True),
                    end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
                    freq=pd.Timedelta(seconds=daily.Interval()),
                    inclusive="left"
                )
            }
            daily_data["temp_max"] = daily.Variables(0).ValuesAsNumpy()
            daily_data["temp_min"] = daily.Variables(1).ValuesAsNumpy()
            daily_data["rain"] = daily.Variables(2).ValuesAsNumpy()
            daily_data["solar_radiation"] = daily.Variables(3).ValuesAsNumpy()
            
            df = pd.DataFrame(data=daily_data)
            df["city"] = city
            all_data.append(df)
            
            # Sleep briefly to be polite to the API
            time.sleep(1) 
            
        except Exception as e:
            print(f"❌ Failed to fetch data for {city}: {e}")

    if all_data:
        final_df = pd.concat(all_data)
        
        # Save raw data
        os.makedirs("data", exist_ok=True)
        final_df.to_csv("data/raw_weather.csv", index=False)
        print(f"✅ Data Ingestion Complete. Collected {len(final_df)} days of weather data across {len(LOCATIONS)} cities.")
    else:
        print("❌ No data collected.")

if __name__ == "__main__":
    fetch_data()