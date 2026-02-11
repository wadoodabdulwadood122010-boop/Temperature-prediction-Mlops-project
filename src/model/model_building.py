import pandas as pd
import numpy as np
import os
import pickle
from pathlib import Path

# Scikit-Learn Models
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor, VotingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error

# XGBoost
from xgboost import XGBRegressor

# --- CONFIGURATION ---
BASE_DIR = Path(__file__).resolve().parent.parent.parent
INPUT_PATH = BASE_DIR / "data/processed/features.csv"
# Changed to generic name to match your evaluation script expectation
MODEL_OUTPUT_PATH = BASE_DIR / "models/weather_rf.pkl" 

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
    
    # 2. Define Models
    rf = RandomForestRegressor(n_estimators=200, max_depth=20, min_samples_split=5, n_jobs=-1, random_state=42)
    xgb = XGBRegressor(n_estimators=200, learning_rate=0.05, max_depth=6, subsample=0.8, colsample_bytree=0.8, n_jobs=-1, random_state=42)
    gb = GradientBoostingRegressor(n_estimators=150, learning_rate=0.05, max_depth=5, random_state=42)
    
    # 3. Train Ensemble
    print("\nTraining Voting Regressor (RF + XGB + GB)...")
    ensemble = VotingRegressor(
        estimators=[('rf', rf), ('xgb', xgb), ('gb', gb)],
        weights=[1, 2, 1] 
    )
    ensemble.fit(X_train, y_train)
    print("✅ Training Complete.")

    # 4. Save Model Locally
    # Ensure directory exists
    os.makedirs(MODEL_OUTPUT_PATH.parent, exist_ok=True)
    
    with open(MODEL_OUTPUT_PATH, "wb") as f:
        pickle.dump(ensemble, f)
        
    print(f"💾 Saved local model to: {MODEL_OUTPUT_PATH}")
    
    # 5. Simple Evaluation Output
    y_pred = ensemble.predict(X_test)
    print("\n--- Ensemble Results ---")
    evaluate_model(y_test, y_pred, model_name="Voting Ensemble")

if __name__ == "__main__":
    train_ensemble()