import pandas as pd
import numpy as np
import mlflow
import mlflow.sklearn
from mlflow.tracking import MlflowClient
import os
import joblib
from pathlib import Path

# Scikit-Learn Models
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor, VotingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error

# XGBoost
from xgboost import XGBRegressor

# --- CONFIGURATION ---
BASE_DIR = Path(__file__).resolve().parent.parent.parent
INPUT_PATH = BASE_DIR / "data/processed/features.csv"
# IMPORTANT: This filename must match what is defined in your dvc.yaml
MODEL_OUTPUT_PATH = BASE_DIR / "models/weather_rf.pkl" 

# MLOps Configuration
DAGSHUB_REPO_OWNER = "wadoodabdulwadood122010"
DAGSHUB_REPO_NAME = "Temperature-prediction-Mlops-project"
EXPERIMENT_NAME = "Pakistan_Weather_Forecasting"
REGISTERED_MODEL_NAME = "Pakistan-Weather-Forecast"

# -------------------------------------------------------------------------------------
# Set up DagsHub credentials for MLflow tracking
dagshub_token = os.getenv("DAGSHUB_TOCKEN")
if not dagshub_token:
    raise EnvironmentError("DAGSHUB_TOCKEN environment variable is not set")

os.environ["MLFLOW_TRACKING_USERNAME"] = dagshub_token
os.environ["MLFLOW_TRACKING_PASSWORD"] = dagshub_token

dagshub_url = "https://dagshub.com"
mlflow.set_tracking_uri(f'{dagshub_url}/{DAGSHUB_REPO_OWNER}/{DAGSHUB_REPO_NAME}.mlflow')
mlflow.set_experiment(EXPERIMENT_NAME)

# -------------------------------------------------------------------------------------

def load_and_split_data(filepath):
    if not filepath.exists():
        raise FileNotFoundError(f"Processed data not found at {filepath}")

    df = pd.read_csv(filepath)
    target = 'temp_max'
    
    # Drop non-feature columns
    drop_cols = [target, 'date', 'year']
    feature_cols = [c for c in df.columns if c not in drop_cols]
    
    # Time-Series Split
    train_mask = df['year'] < 2023
    test_mask = df['year'] >= 2023
    
    X_train = df.loc[train_mask, feature_cols]
    y_train = df.loc[train_mask, target]
    
    X_test = df.loc[test_mask, feature_cols]
    y_test = df.loc[test_mask, target]
    
    print(f"Data Loaded: {len(feature_cols)} features.")
    return X_train, y_train, X_test, y_test, feature_cols

def evaluate_model(y_true, y_pred, model_name="Model"):
    mae = mean_absolute_error(y_true, y_pred)
    rmse = mean_squared_error(y_true, y_pred, squared=False)
    print(f"[{model_name}] Performance: MAE={mae:.4f} | RMSE={rmse:.4f}")
    return mae, rmse

def train_ensemble():
    print("🚀 Initializing Ensemble Training Pipeline...")
    
    # 1. Load Data
    X_train, y_train, X_test, y_test, features = load_and_split_data(INPUT_PATH)
    
    # 2. Start MLflow Run
    with mlflow.start_run() as run:
        
        # --- DEFINING THE MODELS ---
        rf = RandomForestRegressor(n_estimators=200, max_depth=20, min_samples_split=5, n_jobs=-1, random_state=42)
        xgb = XGBRegressor(n_estimators=200, learning_rate=0.05, max_depth=6, subsample=0.8, colsample_bytree=0.8, n_jobs=-1, random_state=42)
        gb = GradientBoostingRegressor(n_estimators=150, learning_rate=0.05, max_depth=5, random_state=42)
        
        # --- TRAINING ---
        print("\nTraining Voting Regressor (RF + XGB + GB)...")
        ensemble = VotingRegressor(
            estimators=[('rf', rf), ('xgb', xgb), ('gb', gb)],
            weights=[1, 2, 1] 
        )
        ensemble.fit(X_train, y_train)
        print("✅ Training Complete.")

        # --- SAVE MODEL LOCALLY (CRITICAL FIX) ---
        # This creates the models/ directory if missing and saves the file
        os.makedirs(MODEL_OUTPUT_PATH.parent, exist_ok=True)
        joblib.dump(ensemble, MODEL_OUTPUT_PATH)
        print(f"💾 Saved local model to: {MODEL_OUTPUT_PATH}")
        
        # --- EVALUATION ---
        y_pred = ensemble.predict(X_test)
        
        print("\n--- Ensemble Results ---")
        mae, rmse = evaluate_model(y_test, y_pred, model_name="Voting Ensemble")
        
        # --- LOGGING TO MLFLOW ---
        mlflow.log_metric("mae", mae)
        mlflow.log_metric("rmse", rmse)
        mlflow.log_param("model_type", "VotingRegressor_RF_XGB_GB")
        mlflow.log_param("weights", "1, 2, 1")

        # --- REGISTER MODEL & PROMOTE TO STAGING ---
        print(f"\n📦 Registering model: '{REGISTERED_MODEL_NAME}'...")
        
        # 1. Log the model artifact and register it
        mlflow.sklearn.log_model(
            sk_model=ensemble,
            artifact_path="model",
            registered_model_name=REGISTERED_MODEL_NAME
        )
        
        # 2. Promote to "Staging"
        client = MlflowClient()
        
        # Robust way to get the latest version (handling race conditions)
        versions = client.search_model_versions(f"name='{REGISTERED_MODEL_NAME}'")
        latest_version = sorted(versions, key=lambda x: int(x.version))[-1].version

        print(f"🔄 Transitioning version {latest_version} to 'Staging'...")
        client.transition_model_version_stage(
            name=REGISTERED_MODEL_NAME,
            version=latest_version,
            stage="Staging",
            archive_existing_versions=True
        )
        
        print(f"✅ Success! Model version {latest_version} is now in 'Staging'.")
        print("💡 Go to DagsHub > Experiments to see your registered model.")

if __name__ == "__main__":
    train_ensemble()