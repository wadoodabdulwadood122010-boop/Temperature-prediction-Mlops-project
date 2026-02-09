from flask import Flask, request, render_template_string
import pandas as pd
import numpy as np
import pickle
import openmeteo_requests
import requests_cache
from retry_requests import retry
from datetime import datetime, timedelta
import mlflow
import dagshub
import os

app = Flask(__name__)

# --- CONFIGURATION ---
DAGSHUB_REPO_OWNER = "wadoodabdulwadood122010"
DAGSHUB_REPO_NAME = "Temperature-prediction-Mlops-project"
MODEL_NAME = "Pakistan-Weather-Forecast"

# 1. Initialize DagsHub Connection
try:
    dagshub.init(repo_owner=DAGSHUB_REPO_OWNER, repo_name=DAGSHUB_REPO_NAME, mlflow=True)
    mlflow.set_tracking_uri(f'https://dagshub.com/{DAGSHUB_REPO_OWNER}/{DAGSHUB_REPO_NAME}.mlflow')
except Exception as e:
    print(f"⚠️ DagsHub Init Warning: {e}")

# 2. Define Paths
ENCODER_PATH = "models/city_encoder.pkl"
LOCAL_MODEL_PATH = "models/weather_rf.pkl" 

# --- LOAD RESOURCES ---
print("🔌 Connecting to DagsHub...")

model = None
city_encoder = None

# Load Model (Remote with Local Fallback)
try:
    print(f"⏳ Attempting to load 'Production' model from DagsHub...")
    model_uri = f"models:/{MODEL_NAME}/Production"
    model = mlflow.sklearn.load_model(model_uri)
    print("✅ Remote Production Model Loaded Successfully!")
except Exception as e:
    print(f"⚠️  REMOTE LOAD FAILED: {e}")
    print("🔄 Switching to Local Fallback...")
    if os.path.exists(LOCAL_MODEL_PATH):
        with open(LOCAL_MODEL_PATH, "rb") as f:
            model = pickle.load(f)
        print(f"✅ Local Model Loaded Successfully from {LOCAL_MODEL_PATH}!")
    else:
        raise FileNotFoundError("❌ CRITICAL: Could not load model from DagsHub OR Locally.")

# Load City Encoder
if os.path.exists(ENCODER_PATH):
    try:
        with open(ENCODER_PATH, "rb") as f:
            city_encoder = pickle.load(f)
        print(f"✅ City Encoder Loaded from {ENCODER_PATH}")
    except Exception as e:
        print(f"❌ Failed to load encoder: {e}")
else:
    print(f"⚠️ Warning: City encoder not found at {ENCODER_PATH}")

# --- WEATHER API SETUP ---
LOCATIONS = {
    "Rawalpindi": {"lat": 33.6007, "lon": 73.0679},
    "Islamabad":  {"lat": 33.6844, "lon": 73.0479},
    "Lahore":     {"lat": 31.5497, "lon": 74.3436},
    "Karachi":    {"lat": 24.8607, "lon": 67.0011},
    "Peshawar":   {"lat": 34.0151, "lon": 71.5249},
    "Quetta":     {"lat": 30.1798, "lon": 66.9750},
    "Multan":     {"lat": 30.1575, "lon": 71.5249},
    "Faisalabad": {"lat": 31.4504, "lon": 73.1350},
    "Sialkot":    {"lat": 32.4945, "lon": 74.5229},
    "Bahawalpur": {"lat": 29.3544, "lon": 71.6911},
    "Hyderabad":  {"lat": 25.3960, "lon": 68.3578},
    "Sukkur":     {"lat": 27.7131, "lon": 68.8524},
    "Gwadar":     {"lat": 25.1216, "lon": 62.3254},
    "Abbottabad": {"lat": 34.1688, "lon": 73.2215},
    "Gilgit":     {"lat": 35.9208, "lon": 74.3089},
    "Skardu":     {"lat": 35.2951, "lon": 75.6337}
}

cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

def get_live_features(city_name):
    coords = LOCATIONS.get(city_name)
    if not coords:
        return None

    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=10)

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": coords["lat"],
        "longitude": coords["lon"],
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d"),
        "daily": ["temperature_2m_max", "temperature_2m_min", "rain_sum", "shortwave_radiation_sum"],
        "timezone": "auto"
    }

    try:
        responses = openmeteo.weather_api(url, params=params)
        daily = responses[0].Daily()
        
        temp_max = daily.Variables(0).ValuesAsNumpy()
        temp_min = daily.Variables(1).ValuesAsNumpy()
        solar = daily.Variables(3).ValuesAsNumpy()
        
        # Calculate features (Lags & Rolling Averages)
        features = {
            'temp_max_lag_1': temp_max[-1],
            'temp_max_lag_3': temp_max[-3],
            'temp_max_lag_7': temp_max[-7],
            'temp_min_lag_1': temp_min[-1],
            'temp_min_lag_3': temp_min[-3],
            'temp_min_lag_7': temp_min[-7],
            'rolling_temp_max_7': np.mean(temp_max[-7:]),
            'rolling_temp_min_7': np.mean(temp_min[-7:]),
            'solar_radiation': np.mean(solar[-3:]),
            'rain': 0.0
        }
        return features
    except Exception as e:
        print(f"Error fetching live data: {e}")
        return None

# --- HTML TEMPLATE ---
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Pakistan Weather Predictor (MLOps)</title>
    <style>
        body { font-family: sans-serif; text-align: center; padding: 50px; background-color: #f4f4f9; }
        .container { background: white; padding: 30px; border-radius: 10px; box-shadow: 0 0 10px rgba(0,0,0,0.1); max-width: 500px; margin: auto; }
        select, button { padding: 10px; width: 100%; margin-top: 20px; font-size: 16px; }
        button { background-color: #28a745; color: white; border: none; cursor: pointer; }
        button:hover { background-color: #218838; }
        .result { margin-top: 20px; font-size: 24px; font-weight: bold; color: #333; }
        .footer { margin-top: 40px; font-size: 12px; color: #777; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🇵🇰 Weather AI</h1>
        <p>Predict tomorrow's temperature using Real-Time MLOps</p>
        <form action="/predict" method="post">
            <select name="city">
                {% for c in cities %}
                <option value="{{ c }}">{{ c }}</option>
                {% endfor %}
            </select>
            <button type="submit">Predict Temperature</button>
        </form>
        {% if prediction %}
        <div class="result">
            City: {{ city }}<br>
            Prediction: {{ prediction }}
        </div>
        {% endif %}
    </div>
    <div class="footer">Powered by XGBoost/RandomForest & Open-Meteo</div>
</body>
</html>
"""

@app.route('/', methods=['GET'])
def index():
    cities = sorted(LOCATIONS.keys())
    return render_template_string(HTML_TEMPLATE, cities=cities, prediction=None)

@app.route('/predict', methods=['POST'])
def predict():
    city = request.form.get('city')
    
    print(f"Fetching live data for {city}...")
    live_features = get_live_features(city)
    
    if not live_features:
        return render_template_string(HTML_TEMPLATE, cities=sorted(LOCATIONS.keys()), prediction="Error fetching data")

    today = datetime.now()
    
    # 1. ENCODE CITY 
    city_encoded = 0
    if city_encoder:
        try:
            city_encoded = city_encoder.transform([city])[0]
        except Exception as e:
            print(f"Encoder error: {e}")
            city_encoded = 0
    
    # 2. FEATURE ENGINEERING (Cyclical Month)
    month_num = today.month
    month_sin = np.sin(2 * np.pi * month_num / 12)
    month_cos = np.cos(2 * np.pi * month_num / 12)
        
    # 3. CONSTRUCT DATAFRAME
    input_data = pd.DataFrame([{
        'city_encoded': city_encoded,
        'day_of_year': today.timetuple().tm_yday + 1,
        'month_cos': month_cos,
        'month_sin': month_sin,
        'temp_max_lag_1': live_features['temp_max_lag_1'],
        'temp_max_lag_3': live_features['temp_max_lag_3'],
        'temp_max_lag_7': live_features['temp_max_lag_7'],
        'temp_min_lag_1': live_features['temp_min_lag_1'],
        'temp_min_lag_3': live_features['temp_min_lag_3'],
        'temp_min_lag_7': live_features['temp_min_lag_7'],
        'rolling_temp_max_7': live_features['rolling_temp_max_7'],
        'rolling_temp_min_7': live_features['rolling_temp_min_7'],
        'solar_radiation': live_features['solar_radiation'],
        'rain': live_features['rain']
    }])
    
    # 4. FIX COLUMN ORDER 
    # This aligns the columns to exactly what the model expects
    if hasattr(model, 'feature_names_in_'):
        try:
            input_data = input_data[model.feature_names_in_]
        except KeyError as e:
            return render_template_string(HTML_TEMPLATE, cities=sorted(LOCATIONS.keys()), prediction=f"Missing Columns: {e}")

    try:
        prediction = model.predict(input_data)[0]
        return render_template_string(HTML_TEMPLATE, cities=sorted(LOCATIONS.keys()), city=city, prediction=f"{round(prediction, 1)}°C")
    except Exception as e:
        print(f"Prediction Error: {e}")
        return render_template_string(HTML_TEMPLATE, cities=sorted(LOCATIONS.keys()), prediction=f"Error: {e}")

if __name__ == '__main__':
    app.run(debug=True, port=5000)