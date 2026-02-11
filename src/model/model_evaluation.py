import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import mlflow
import mlflow.sklearn
import json
import pickle
from pathlib import Path
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import os

# --- CONFIGURATION ---
dagshub_token = os.getenv("DAGSHUB_TOCKEN")
if not dagshub_token:
    raise EnvironmentError("DAGSHUB_TOCKEN environment variable is not set")

os.environ["MLFLOW_TRACKING_USERNAME"] = dagshub_token
os.environ["MLFLOW_TRACKING_PASSWORD"] = dagshub_token

dagshub_url = "https://dagshub.com"
repo_owner = "wadoodabdulwadood122010"
repo_name = "Temperature-prediction-Mlops-project"
mlflow.set_tracking_uri(f'{dagshub_url}/{repo_owner}/{repo_name}.mlflow')
mlflow.set_experiment("Using_votting_regresor")

# Define Paths
BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_PATH = BASE_DIR / "data/processed/features.csv"
MODEL_PATH = BASE_DIR / "models/weather_rf.pkl"  # Make sure this matches model_building output
IMG_DIR = BASE_DIR / "reports/figures"
METRICS_PATH = BASE_DIR / "reports/metrics.json"

def load_test_data():
    if not DATA_PATH.exists():
        raise FileNotFoundError(f"❌ Data file not found at: {DATA_PATH}")

    df = pd.read_csv(DATA_PATH)
    test_df = df[df['year'] >= 2023].copy()
    
    target = 'temp_max'
    drop_cols = [c for c in [target, 'date', 'year'] if c in test_df.columns]
    
    X_test = test_df.drop(columns=drop_cols)
    y_test = test_df[target]
    
    return X_test, y_test

def plot_residuals(y_true, y_pred):
    IMG_DIR.mkdir(parents=True, exist_ok=True)
    
    fig, axes = plt.subplots(1, 2, figsize=(15, 6))
    
    sns.scatterplot(x=y_true, y=y_pred, alpha=0.5, ax=axes[0])
    min_val = min(y_true.min(), y_pred.min())
    max_val = max(y_true.max(), y_pred.max())
    axes[0].plot([min_val, max_val], [min_val, max_val], 'r--', lw=2)
    axes[0].set_title("Actual vs Predicted")
    
    residuals = y_true - y_pred
    sns.histplot(residuals, kde=True, ax=axes[1], color='orange')
    axes[1].axvline(0, color='r', linestyle='--')
    axes[1].set_title("Residual Distribution")
    
    save_path = IMG_DIR / "model_evaluation_plot.png"
    plt.tight_layout()
    plt.savefig(save_path)
    plt.close()
    
    return save_path

def evaluate():
    print("🚀 Starting Model Evaluation...")
    
    # 1. Load Data & Model
    X_test, y_test = load_test_data()
    print(f"   Loaded {len(X_test)} test samples.")
    print(f"⏳ Loading model from local path: {MODEL_PATH}...")
    
    if not MODEL_PATH.exists():
        raise FileNotFoundError(f"❌ Model file not found at: {MODEL_PATH}")

    with open(MODEL_PATH, "rb") as f:
        model = pickle.load(f)
    print("✅ Local Model Loaded Successfully!")

    # 2. Predict & Metrics
    y_pred = model.predict(X_test)
    mae = mean_absolute_error(y_test, y_pred)
    rmse = mean_squared_error(y_test, y_pred, squared=False)
    r2 = r2_score(y_test, y_pred)
    
    print(f"   📊 Performance: MAE={mae:.4f} | RMSE={rmse:.4f} | R2={r2:.4f}")

    # 3. Log to MLflow (CRITICAL FOR REGISTRY STEP)
    with mlflow.start_run(run_name="Local_Model_Evaluation") as run:
        # A. Log Metrics
        mlflow.log_metric("eval_mae", mae)
        mlflow.log_metric("eval_rmse", rmse)
        mlflow.log_metric("eval_r2", r2)
        
        # B. Log Plot
        plot_path = plot_residuals(y_test, y_pred)
        mlflow.log_artifact(str(plot_path))
        
        # C. Log Model (So registry script can find it!)
        mlflow.sklearn.log_model(model, "model")
        print(f"   ☁️  Model uploaded to MLflow (Run ID: {run.info.run_id})")

        # 4. Save Metrics to JSON with Run ID
        metrics_dict = {
            "mae": mae,
            "rmse": rmse,
            "r2": r2,
            "run_id": run.info.run_id  # <--- THIS FIXES THE ERROR
        }
        
        METRICS_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(METRICS_PATH, "w") as f:
            json.dump(metrics_dict, f, indent=4)
            
        print(f"   📄 Metrics & Run ID saved to: {METRICS_PATH}")

if __name__ == "__main__":
    evaluate()