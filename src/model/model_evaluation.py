import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import mlflow
import mlflow.sklearn
from mlflow.tracking import MlflowClient
import dagshub
import json
from pathlib import Path
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import os
# --- CONFIGURATION ---
DAGSHUB_REPO_OWNER = "wadoodabdulwadood122010" 
DAGSHUB_REPO_NAME = "Temperature-prediction-Mlops-project"
REGISTERED_MODEL_NAME = "Pakistan-Weather-Forecast" # Must match model_building.py
# Set up DagsHub credentials for MLflow tracking
dagshub_token = os.getenv("DAGSHUB_TOCKEN")
if not dagshub_token:
    raise EnvironmentError("DAGSHUB_TOCKEN environment variable is not set")

os.environ["MLFLOW_TRACKING_USERNAME"] = dagshub_token
os.environ["MLFLOW_TRACKING_PASSWORD"] = dagshub_token

dagshub_url = "https://dagshub.com"
repo_owner = "wadoodabdulwadood122010"
repo_name = "Temperature-prediction-Mlops-project"
mlflow.set_tracking_uri(f'{dagshub_url}/{repo_owner}/{repo_name}.mlflow')
# -------------------------------------------------------------------------------------
# Initialize DagsHub & MLflow
# dagshub.init(repo_owner=DAGSHUB_REPO_OWNER, repo_name=DAGSHUB_REPO_NAME, mlflow=True)
# mlflow.set_tracking_uri(f'https://dagshub.com/{DAGSHUB_REPO_OWNER}/{DAGSHUB_REPO_NAME}.mlflow')
# mlflow.set_experiment("MODEL_EVALUATION")

# Define Paths
BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_PATH = BASE_DIR / "data/processed/features.csv"
IMG_DIR = BASE_DIR / "reports/figures"
METRICS_PATH = BASE_DIR / "reports/metrics.json"

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

    # 2. Load Model from MLflow STAGING
    # We dynamically fetch whatever is currently in 'Staging'
    model_uri = f"models:/{REGISTERED_MODEL_NAME}/Staging"
    print(f"⏳ Downloading 'Staging' model from DagsHub ({model_uri})...")
    
    try:
        model = mlflow.sklearn.load_model(model_uri)
        print("✅ Staging Model Loaded Successfully!")
    except Exception as e:
        print(f"❌ Failed to load model. Ensure a model is in 'Staging'. Error: {e}")
        return

    # 3. Get Model Version (for logging)
    client = MlflowClient()
    latest_version_info = client.get_latest_versions(REGISTERED_MODEL_NAME, stages=["Staging"])
    staging_version = latest_version_info[0].version if latest_version_info else "Unknown"
    print(f"   Evaluating Model Version: {staging_version}")

    # 4. Predict
    y_pred = model.predict(X_test)
    
    # 5. Calculate Metrics
    mae = mean_absolute_error(y_test, y_pred)
    rmse = mean_squared_error(y_test, y_pred, squared=False)
    r2 = r2_score(y_test, y_pred)
    
    print(f"   📊 Performance:")
    print(f"      MAE:  {mae:.4f}")
    print(f"      RMSE: {rmse:.4f}")
    print(f"      R2:   {r2:.4f}")

    # 6. Log to MLflow
    with mlflow.start_run(run_name=f"Eval_Version_{staging_version}") as run:
        mlflow.log_metric("eval_mae", mae)
        mlflow.log_metric("eval_rmse", rmse)
        mlflow.log_metric("eval_r2", r2)
        mlflow.log_param("evaluated_version", staging_version)
        
        # Log Plot
        plot_path = plot_residuals(y_test, y_pred)
        mlflow.log_artifact(str(plot_path))
        print(f"   🖼️ Plots logged to DagsHub.")

        # 7. Save Metrics to JSON (Crucial for Next Step)
        # We save the 'staging_version' so the next script knows WHAT to promote to Production
        metrics_dict = {
            "mae": mae,
            "rmse": rmse,
            "r2": r2,
            "staging_version": staging_version,
            "run_id": run.info.run_id
        }
        
        METRICS_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(METRICS_PATH, "w") as f:
            json.dump(metrics_dict, f, indent=4)
            
        print(f"   📄 Metrics & Version Info saved to: {METRICS_PATH}")

if __name__ == "__main__":
    evaluate()