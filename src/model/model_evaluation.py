import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import mlflow
import json
import pickle
from pathlib import Path
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import os

# --- CONFIGURATION ---
# DagsHub/MLflow auth is kept for logging the results, even if loading is local
dagshub_token = os.getenv("DAGSHUB_TOCKEN")
if not dagshub_token:
    raise EnvironmentError("DAGSHUB_TOCKEN environment variable is not set")

os.environ["MLFLOW_TRACKING_USERNAME"] = dagshub_token
os.environ["MLFLOW_TRACKING_PASSWORD"] = dagshub_token

dagshub_url = "https://dagshub.com"
repo_owner = "wadoodabdulwadood122010"
repo_name = "Temperature-prediction-Mlops-project"
mlflow.set_tracking_uri(f'{dagshub_url}/{repo_owner}/{repo_name}.mlflow')

# Define Paths
BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_PATH = BASE_DIR / "data/processed/features.csv"
# Changed: Path to local pickle model
MODEL_PATH = BASE_DIR / "models/weather_rf.pkl"  
IMG_DIR = BASE_DIR / "reports/figures"
# Changed: Path to local metrics JSON
METRICS_PATH = BASE_DIR / "reports/metrics.json"
experiment_name = "Using_votting_regresor"

def load_test_data():
    """Re-creates the test split used during training (2023+ data)."""
    if not DATA_PATH.exists():
        raise FileNotFoundError(f"❌ Data file not found at: {DATA_PATH}")

    df = pd.read_csv(DATA_PATH)
    
    # Filter for Test Data (2023 onwards)
    test_df = df[df['year'] >= 2023].copy()
    
    target = 'temp_max'
    # Drop non-feature columns
    drop_cols = [c for c in [target, 'date', 'year'] if c in test_df.columns]
    
    X_test = test_df.drop(columns=drop_cols)
    y_test = test_df[target]
    
    return X_test, y_test

def plot_residuals(y_true, y_pred):
    """Plots Actual vs Predicted and Residual Distribution."""
    IMG_DIR.mkdir(parents=True, exist_ok=True)
    
    fig, axes = plt.subplots(1, 2, figsize=(15, 6))
    
    # 1. Actual vs Predicted
    sns.scatterplot(x=y_true, y=y_pred, alpha=0.5, ax=axes[0])
    min_val = min(y_true.min(), y_pred.min())
    max_val = max(y_true.max(), y_pred.max())
    axes[0].plot([min_val, max_val], [min_val, max_val], 'r--', lw=2)
    axes[0].set_title("Actual vs Predicted Temperature")
    axes[0].set_xlabel("Actual (°C)")
    axes[0].set_ylabel("Predicted (°C)")
    
    # 2. Residuals (Errors)
    residuals = y_true - y_pred
    sns.histplot(residuals, kde=True, ax=axes[1], color='orange')
    axes[1].axvline(0, color='r', linestyle='--')
    axes[1].set_title("Residual Distribution (Error)")
    axes[1].set_xlabel("Error (°C)")
    
    save_path = IMG_DIR / "model_evaluation_plot.png"
    plt.tight_layout()
    plt.savefig(save_path)
    plt.close()
    
    return save_path

def evaluate():
    print("🚀 Starting Model Evaluation...")
    
    # 1. Load Test Data
    X_test, y_test = load_test_data()
    print(f"   Loaded {len(X_test)} test samples.")

    # 2. Load Model from Local Pickle
    print(f"⏳ Loading model from local path: {MODEL_PATH}...")
    
    if not MODEL_PATH.exists():
        raise FileNotFoundError(f"❌ Model file not found at: {MODEL_PATH}")

    try:
        with open(MODEL_PATH, "rb") as f:
            model = pickle.load(f)
        print("✅ Local Model Loaded Successfully!")
    except Exception as e:
        print(f"❌ Failed to load local model. Error: {e}")
        return

    # 3. Predict
    y_pred = model.predict(X_test)
    
    # 4. Calculate Metrics
    mae = mean_absolute_error(y_test, y_pred)
    rmse = mean_squared_error(y_test, y_pred, squared=False)
    r2 = r2_score(y_test, y_pred)
    
    print(f"   📊 Performance:")
    print(f"      MAE:  {mae:.4f}")
    print(f"      RMSE: {rmse:.4f}")
    print(f"      R2:   {r2:.4f}")

    # 5. Save Metrics to JSON (Local)
    metrics_dict = {
        "mae": mae,
        "rmse": rmse,
        "r2": r2
    }
    
    METRICS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(METRICS_PATH, "w") as f:
        json.dump(metrics_dict, f, indent=4)
        
    print(f"   📄 Metrics saved locally to: {METRICS_PATH}")
    mlflow.set_experiment(experiment_name)
    with mlflow.start_run() as run:
        mlflow.log_metric("eval_mae", mae)
        mlflow.log_metric("eval_rmse", rmse)
        mlflow.log_metric("eval_r2", r2)
        
        # Log Plot
        plot_path = plot_residuals(y_test, y_pred)
        mlflow.log_artifact(str(plot_path))
        print(f"   🖼️ Plots logged to DagsHub.")

if __name__ == "__main__":
    evaluate()