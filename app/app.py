from flask import Flask, request, render_template_string, Response
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
import time
from dotenv import load_dotenv

# --- PROMETHEUS IMPORTS ---
from prometheus_client import Counter, Histogram, generate_latest, Gauge

load_dotenv()
app = Flask(__name__)

# --- PROMETHEUS METRICS DEFINITION ---
REQUEST_COUNT = Counter(
    'prediction_requests_total', 
    'Total number of prediction requests',
    ['city', 'status']
)

PREDICTION_LATENCY = Histogram(
    'prediction_latency_seconds', 
    'Time taken to process prediction',
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0]
)

API_LATENCY = Histogram(
    'weather_api_latency_seconds',
    'Time taken to fetch data from Open-Meteo'
)

PREDICTED_VALUE = Gauge(
    'predicted_temperature_celsius',
    'The predicted temperature value',
    ['city']
)

# --- CONFIGURATION ---
DAGSHUB_REPO_OWNER = "wadoodabdulwadood122010"
DAGSHUB_REPO_NAME = "Temperature-prediction-Mlops-project"
MODEL_NAME = "Pakistan-Weather-Forecast"

# --- AUTHENTICATION & CONNECTION ---
dagshub_token = os.getenv("DAGSHUB_TOCKEN")
if not dagshub_token:
    print("⚠️ WARNING: DAGSHUB_TOCKEN not found. Remote model loading might fail.")
else:
    os.environ["MLFLOW_TRACKING_USERNAME"] = dagshub_token
    os.environ["MLFLOW_TRACKING_PASSWORD"] = dagshub_token

try:
    # dagshub.init(repo_owner=DAGSHUB_REPO_OWNER, repo_name=DAGSHUB_REPO_NAME, mlflow=True)
    mlflow.set_tracking_uri(f'https://dagshub.com/{DAGSHUB_REPO_OWNER}/{DAGSHUB_REPO_NAME}.mlflow')
    print("✅ Connected to DagsHub MLflow Tracking")
except Exception as e:
    print(f"⚠️ DagsHub Init Warning: {e}")

# --- PATHS ---
ENCODER_PATH = "city_encoder.pkl"

# --- LOAD RESOURCES ---
model = None
city_encoder = None

# Load Encoder Locally
if os.path.exists(ENCODER_PATH):
    try:
        with open(ENCODER_PATH, "rb") as f:
            city_encoder = pickle.load(f)
        print(f"✅ Local City Encoder Loaded: {len(city_encoder.classes_)} cities found.")
    except Exception as e:
        print(f"❌ Failed to load encoder: {e}")
else:
    print(f"⚠️ Warning: City encoder not found at {ENCODER_PATH}")

# Load Model from Registry
print(f"🔌 Connecting to Model Registry...")
try:
    print(f"⏳ Downloading '{MODEL_NAME}' (Stage: Production)...")
    model_uri = f"models:/{MODEL_NAME}/Production"
    model = mlflow.sklearn.load_model(model_uri)
    print("✅ Remote Production Model Loaded Successfully!")
except Exception as e:
    print(f"❌ FAILED to load remote model. Error: {e}")

# --- WEATHER API SETUP ---
# UPDATED: Only includes the 15 cities present in your city_encoder.pkl
LOCATIONS = {
    "Abbottabad": {"lat": 34.1688, "lon": 73.2215},
    "Bahawalpur": {"lat": 29.3544, "lon": 71.6911},
    "Faisalabad": {"lat": 31.4504, "lon": 73.1350},
    "Gilgit":     {"lat": 35.9208, "lon": 74.3089},
    "Gwadar":     {"lat": 25.1216, "lon": 62.3254},
    "Hyderabad":  {"lat": 25.3960, "lon": 68.3578},
    "Islamabad":  {"lat": 33.6844, "lon": 73.0479},
    "Karachi":    {"lat": 24.8607, "lon": 67.0011},
    "Lahore":     {"lat": 31.5497, "lon": 74.3436},
    "Multan":     {"lat": 30.1575, "lon": 71.5249},
    "Quetta":     {"lat": 30.1798, "lon": 66.9750},
    "Rawalpindi": {"lat": 33.6007, "lon": 73.0679},
    "Sialkot":    {"lat": 32.4945, "lon": 74.5229},
    "Skardu":     {"lat": 35.2951, "lon": 75.6337},
    "Sukkur":     {"lat": 27.7131, "lon": 68.8524}
}

cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

def get_live_features(city_name):
    coords = LOCATIONS.get(city_name)
    if not coords:
        return None

    start_time = time.time()
    
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
        API_LATENCY.observe(time.time() - start_time)
        
        daily = responses[0].Daily()
        
        temp_max = daily.Variables(0).ValuesAsNumpy()
        temp_min = daily.Variables(1).ValuesAsNumpy()
        solar = daily.Variables(3).ValuesAsNumpy()
        
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
    <title>Pakistan Weather Predictor</title>
    <style>
        body { font-family: sans-serif; text-align: center; padding: 50px; background-color: #f4f4f9; }
        .container { background: white; padding: 30px; border-radius: 10px; box-shadow: 0 0 10px rgba(0,0,0,0.1); max-width: 500px; margin: auto; }
        select, button { padding: 10px; width: 100%; margin-top: 20px; font-size: 16px; }
        button { background-color: #28a745; color: white; border: none; cursor: pointer; }
        button:hover { background-color: #218838; }
        .result { margin-top: 20px; font-size: 24px; font-weight: bold; color: #333; }
        .metrics-link { margin-top: 20px; display: block; color: #007bff; text-decoration: none; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🇵🇰 Weather AI</h1>
        <form action="/predict" method="post">
            <select name="city">
                {% for c in cities %}
                <option value="{{ c }}" {% if city == c %}selected{% endif %}>{{ c }}</option>
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
        <a href="/metrics" class="metrics-link">View Metrics</a>
    </div>
</body>
</html>
"""

@app.route('/', methods=['GET'])
def index():
    # Sort cities alphabetically for the dropdown
    cities = sorted(LOCATIONS.keys())
    return render_template_string(HTML_TEMPLATE, cities=cities, prediction=None)

@app.route('/metrics')
def metrics():
    return Response(generate_latest(), mimetype='text/plain')

@app.route('/predict', methods=['POST'])
def predict():
    start_time = time.time()
    city = request.form.get('city')
    
    # 1. Validate City against Encoder immediately
    if not city_encoder:
        return render_template_string(HTML_TEMPLATE, cities=sorted(LOCATIONS.keys()), prediction="Error: Encoder not loaded")
    
    if city not in city_encoder.classes_:
        REQUEST_COUNT.labels(city=city, status='error_unknown_city').inc()
        return render_template_string(HTML_TEMPLATE, cities=sorted(LOCATIONS.keys()), prediction=f"Error: Unknown city '{city}'")

    # 2. Fetch Live Data
    print(f"Fetching live data for {city}...")
    live_features = get_live_features(city)
    
    if not live_features:
        REQUEST_COUNT.labels(city=city, status='failure_api').inc()
        return render_template_string(HTML_TEMPLATE, cities=sorted(LOCATIONS.keys()), prediction="Error fetching weather data")

    # 3. Preprocessing
    today = datetime.now()
    
    try:
        # Transform city using the loaded pickle file
        city_encoded = city_encoder.transform([city])[0]
        
        # Date Features
        month_num = today.month
        month_sin = np.sin(2 * np.pi * month_num / 12)
        month_cos = np.cos(2 * np.pi * month_num / 12)
        day_of_year = today.timetuple().tm_yday + 1

        # Prepare DataFrame
        input_data = pd.DataFrame([{
            'city_encoded': city_encoded,
            'day_of_year': day_of_year,
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
        
        # Align columns with model
        if hasattr(model, 'feature_names_in_'):
            input_data = input_data[model.feature_names_in_]

        # 4. Prediction
        prediction = model.predict(input_data)[0]
        
        # Metrics Update
        REQUEST_COUNT.labels(city=city, status='success').inc()
        PREDICTION_LATENCY.observe(time.time() - start_time)
        PREDICTED_VALUE.labels(city=city).set(prediction)
        
        return render_template_string(HTML_TEMPLATE, cities=sorted(LOCATIONS.keys()), city=city, prediction=f"{round(prediction, 1)}°C")
    
    except Exception as e:
        print(f"Prediction Error: {e}")
        REQUEST_COUNT.labels(city=city, status='error_predict').inc()
        return render_template_string(HTML_TEMPLATE, cities=sorted(LOCATIONS.keys()), prediction=f"Error: {e}")

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)