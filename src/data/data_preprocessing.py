import pandas as pd
import numpy as np
import pickle
from pathlib import Path
from sklearn.preprocessing import LabelEncoder

# --- Configuration ---
RAW_DATA_PATH = Path("data/raw/weather.csv")
PROCESSED_DATA_PATH = Path("data/interim/features.csv")
ENCODER_PATH = Path("models/city_encoder.pkl")
MODEL_DIR = Path("models")

def load_data(filepath):
    if not filepath.exists():
        raise FileNotFoundError(f"Raw data not found at {filepath}. Run ingestion first.")
    
    df = pd.read_csv(filepath)
    df['date'] = pd.to_datetime(df['date'])
    return df

def generate_time_signals(df):
    """
    Adds cyclical time features.
    Models struggle with 12 (Dec) jumping to 1 (Jan). 
    Sine/Cosine transformations make the transition smooth.
    """
    df['day_of_year'] = df['date'].dt.dayofyear
    df['month'] = df['date'].dt.month
    
    # Cyclical encoding for seasonality
    df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
    df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)
    
    return df

def generate_lag_features(df):
    """
    Creates lag features to help the model learn temporal dependencies.
    """
    # Sort is critical before shifting
    df = df.sort_values(by=['city', 'date']).reset_index(drop=True)
    
    # Group by city so lags don't bleed between different locations
    grouper = df.groupby('city')['temp_max']
    
    # Direct Lags (Past values)
    df['temp_max_lag_1'] = grouper.shift(1)
    df['temp_max_lag_3'] = grouper.shift(3)
    df['temp_max_lag_7'] = grouper.shift(7)
    
    # Rolling Statistics (Trends)
    df['temp_rolling_mean_7'] = grouper.transform(lambda x: x.rolling(7).mean())
    df['temp_rolling_std_7'] = grouper.transform(lambda x: x.rolling(7).std())
    
    return df

def encode_categoricals(df):
    """
    Encodes city names into integers and saves the encoder for inference.
    """
    le = LabelEncoder()
    df['city_encoded'] = le.fit_transform(df['city'])
    
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    
    with open(ENCODER_PATH, "wb") as f:
        pickle.dump(le, f)
        
    return df

def preprocess_data():
    print("Starting Data Preprocessing...")
    
    # 1. Load
    df = load_data(RAW_DATA_PATH)
    initial_count = len(df)
    
    # 2. Feature Engineering
    df = generate_time_signals(df)
    df = generate_lag_features(df)
    df = encode_categoricals(df)
    
    # 3. Cleaning
    # Drop rows with NaNs created by lags (first 7 days of every city)
    df = df.dropna()
    dropped_count = initial_count - len(df)
    
    # 4. Save
    PROCESSED_DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(PROCESSED_DATA_PATH, index=False)
    
    print("-" * 30)
    print("Preprocessing Complete.")
    print(f"Dropped rows (NaNs): {dropped_count}")
    print(f"Final dataset shape: {df.shape}")
    print(f"Saved to: {PROCESSED_DATA_PATH}")
    print(f"Encoder saved to: {ENCODER_PATH}")

if __name__ == "__main__":
    preprocess_data()