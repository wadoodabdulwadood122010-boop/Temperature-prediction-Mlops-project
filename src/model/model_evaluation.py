import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import pickle
import mlflow
import dagshub
import json
from pathlib import Path
import os
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

# --- CONFIGURATION ---

#production
# dagshub_token = os.getenv("CAPSTONE_TEST")
# if not dagshub_token:
#     raise EnvironmentError("CAPSTONE_TEST environment variable is not set")

# os.environ["MLFLOW_TRACKING_USERNAME"] = dagshub_token
# os.environ["MLFLOW_TRACKING_PASSWORD"] = dagshub_token

# dagshub_url = "https://dagshub.com"
# repo_owner = "vikashdas770"
# repo_name = "YT-Capstone-Project"
# mlflow.set_tracking_uri(f'{dagshub_url}/{repo_owner}/{repo_name}.mlflow')
# UPDATE THESE WITH YOUR DAGSHUB INFO
DAGSHUB_REPO_OWNER = "wadoodabdulwadood122010" 
DAGSHUB_REPO_NAME = "Temperature-prediction-Mlops-project"
dagshub.init(repo_owner=DAGSHUB_REPO_OWNER, repo_name=DAGSHUB_REPO_NAME, mlflow=True)
mlflow.set_tracking_uri(f'https://dagshub.com/{DAGSHUB_REPO_OWNER}/{DAGSHUB_REPO_NAME}.mlflow')
mlflow.set_experiment("MODEL_EVALUATION")
# Define Paths relative to the project root
BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_PATH = BASE_DIR / "data/processed/features.csv"
MODEL_PATH = BASE_DIR / "models/weather_rf.pkl"
IMG_DIR = BASE_DIR / "reports/figures"
METRICS_PATH = BASE_DIR / "reports/metrics.json"

def load_test_data():
    """Re-creates the test split used during training."""
    if not DATA_PATH.exists():
        raise FileNotFoundError(f"❌ Data file not found at: {DATA_PATH}")

    df = pd.read_csv(DATA_PATH)
    
    # Deterministic split based on year
    test_df = df[df['year'] >= 2023].copy()
    
    target = 'temp_max'
    drop_cols = [c for c in [target, 'date', 'year'] if c in test_df.columns]
    
    X_test = test_df.drop(columns=drop_cols)
    y_test = test_df[target]
    
    return X_test, y_test

def plot_residuals(y_true, y_pred):
    """
    Plots Actual vs Predicted and Residual Distribution.
    """
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
    
    # 1. Load Resources
    X_test, y_test = load_test_data()
    
    if not MODEL_PATH.exists():
        raise FileNotFoundError(f"❌ Model file not found at: {MODEL_PATH}")

    with open(MODEL_PATH, "rb") as f:
        model = pickle.load(f)
        
    # 2. Predict
    print(f"   Evaluating on {len(X_test)} test samples...")
    y_pred = model.predict(X_test)
    
    # 3. Calculate Metrics
    mae = mean_absolute_error(y_test, y_pred)
    rmse = mean_squared_error(y_test, y_pred, squared=False)
    r2 = r2_score(y_test, y_pred)
    
    print(f"   📊 Results:")
    print(f"      MAE:  {mae:.4f}")
    print(f"      RMSE: {rmse:.4f}")
    print(f"      R2:   {r2:.4f}")

    # 4. Log to MLflow AND Save JSON
    # We use 'as run' to capture the Run ID
    with mlflow.start_run(run_name="Model_Evaluation_Step") as run:
        # Log metrics to MLflow
        mlflow.log_metric("eval_mae", mae)
        print('mae logged!')
        mlflow.log_metric("eval_rmse", rmse)
        print('rmse logged!')
        mlflow.log_metric("eval_r2", r2)
        print('r2 logged!')
        # Log the plot artifact
        plot_path = plot_residuals(y_test, y_pred)
        mlflow.log_artifact(str(plot_path))
        print(f"   🖼️  Plots logged to DagsHub: {plot_path}")

        # mlflow.sklearn.log_model(model, "model")
        # --- IMPORTANT: SAVE METRICS & RUN ID TO JSON ---
        # This allows the registry script to know WHICH run to register
        metrics_dict = {
            "mae": mae,
            "rmse": rmse,
            "r2": r2,
            "run_id": run.info.run_id  # <--- CAPTURING THE RUN ID
        }
        
        METRICS_PATH.parent.mkdir(parents=True, exist_ok=True)
        
        with open(METRICS_PATH, "w") as f:
            json.dump(metrics_dict, f, indent=4)
            
        print(f"   📄 Metrics & Run ID saved to: {METRICS_PATH}")

if __name__ == "__main__":
    evaluate()