import pandas as pd
import numpy as np
import pickle
import mlflow
import mlflow.sklearn
from pathlib import Path

# Scikit-Learn Models
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor, VotingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error

# XGBoost (The "Heavy Lifter")
from xgboost import XGBRegressor

# --- CONFIGURATION ---
BASE_DIR = Path(__file__).resolve().parent.parent.parent
INPUT_PATH = BASE_DIR / "data/processed/features.csv"
MODEL_PATH = BASE_DIR / "models/weather_rf.pkl" # We keep the name same so app.py finds it

def load_and_split_data(filepath):
    if not filepath.exists():
        raise FileNotFoundError(f"Processed data not found at {filepath}")

    df = pd.read_csv(filepath)
    
    target = 'temp_max'
    
    # Drop non-feature columns
    # We remove 'year' to ensure model generalizes to future years
    drop_cols = [target, 'date', 'year']
    feature_cols = [c for c in df.columns if c not in drop_cols]
    
    # Time-Series Split (Train on past, Test on "future")
    train_mask = df['year'] < 2023
    test_mask = df['year'] >= 2023
    
    X_train = df.loc[train_mask, feature_cols]
    y_train = df.loc[train_mask, target]
    
    X_test = df.loc[test_mask, feature_cols]
    y_test = df.loc[test_mask, target]
    
    print(f"Data Loaded: {len(feature_cols)} features.")
    print(f" - Train rows: {X_train.shape[0]}")
    print(f" - Test rows:  {X_test.shape[0]}")
    
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
    
  
        # --- DEFINING THE MODELS ---
        
        # Model 1: Random Forest (Stability)
    rf = RandomForestRegressor(
            n_estimators=200, 
            max_depth=20, 
            min_samples_split=5, 
            n_jobs=-1, 
            random_state=42
        )
        
        # Model 2: XGBoost (High Accuracy)
    xgb = XGBRegressor(
            n_estimators=200,
            learning_rate=0.05,
            max_depth=6,
            subsample=0.8,
            colsample_bytree=0.8,
            n_jobs=-1,
            random_state=42
        )
        
        # Model 3: Gradient Boosting (Error Correction)
    gb = GradientBoostingRegressor(
            n_estimators=150,
            learning_rate=0.05,
            max_depth=5,
            random_state=42
        )
        
        # --- COMBINING MODELS (Voting Regressor) ---
    print("\nTraining Voting Regressor (RF + XGB + GB)...")
        # Weights: We give XGBoost slightly more influence (2x) because it is usually more accurate
    ensemble = VotingRegressor(
            estimators=[('rf', rf), ('xgb', xgb), ('gb', gb)],
            weights=[1, 2, 1] 
        )
        
        # Fit the ensemble
    ensemble.fit(X_train, y_train)
    print("✅ Training Complete.")
        
        # --- EVALUATION ---
    y_pred = ensemble.predict(X_test)
        
        # Baseline Comparison
    if 'temp_max_lag_1' in X_test.columns:
            print("\n--- Baseline Check ---")
            evaluate_model(y_test, X_test['temp_max_lag_1'], model_name="Persistence (Yesterday's Temp)")
            
    print("\n--- Ensemble Results ---")
    mae, rmse = evaluate_model(y_test, y_pred, model_name="Voting Ensemble")
        
        # Log Metrics
    
        # --- SAVING ---
    MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
        
        # Save the Ensemble as a Pickle
        # Note: Since app.py loads a pickle object with .predict(), this works seamlessly
    with open(MODEL_PATH, "wb") as f:
        pickle.dump(ensemble, f)
            
        print(f"\n💾 Ensemble Model saved to: {MODEL_PATH}")

if __name__ == "__main__":
    train_ensemble()