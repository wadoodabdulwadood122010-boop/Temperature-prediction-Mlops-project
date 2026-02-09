import mlflow
from mlflow.tracking import MlflowClient
import dagshub
import os
dagshub_token = os.getenv("DAGSHUB_TOCKEN")
if not dagshub_token:
    raise EnvironmentError("DAGSHUB_TOCKEN environment variable is not set")

os.environ["MLFLOW_TRACKING_USERNAME"] = dagshub_token
os.environ["MLFLOW_TRACKING_PASSWORD"] = dagshub_token

dagshub_url = "https://dagshub.com"
repo_owner = "wadoodabdulwadood122010"
repo_name = "Temperature-prediction-Mlops-project"
mlflow.set_tracking_uri(f'{dagshub_url}/{repo_owner}/{repo_name}.mlflow')



# --- CONFIGURATION ---
# DAGSHUB_REPO_OWNER = "wadoodabdulwadood122010" 
# DAGSHUB_REPO_NAME = "Temperature-prediction-Mlops-project"
MODEL_NAME = "Pakistan-Weather-Forecast"  # Must match the name you used when registering the model
# dagshub.init(repo_owner=DAGSHUB_REPO_OWNER, repo_name=DAGSHUB_REPO_NAME, mlflow=True)
# mlflow.set_tracking_uri(f'https://dagshub.com/{DAGSHUB_REPO_OWNER}/{DAGSHUB_REPO_NAME}.mlflow')
# Metric to compare (Change this if you want to optimize for R2 or RMSE)
METRIC_KEY = "mae"
METRIC_KEY2 = "eval_mae" 
LOWER_IS_BETTER = True  # Set to True for Error metrics (MAE, RMSE), False for Score metrics (R2, Accuracy)

def get_latest_version(client, model_name, stage):
    """
    Helper to get the latest version of a model in a specific stage.
    Returns None if no model is found in that stage.
    """
    versions = client.get_latest_versions(model_name, stages=[stage])
    # Filter to ensure we only get the exact stage we asked for
    # (MLflow sometimes returns empty lists or 'None' stage if not careful)
    versions = [v for v in versions if v.current_stage == stage]
    
    if not versions:
        return None
    # Return the one with the highest version number
    return max(versions, key=lambda x: int(x.version))

def promote_to_production():
    print(f"🚀 Checking models for: {MODEL_NAME}")
    
    # 1. Initialize DagsHub Connection
    
    
    client = MlflowClient()

    # 2. Fetch Models
    staging_model = get_latest_version(client, MODEL_NAME, "Staging")
    production_model = get_latest_version(client, MODEL_NAME, "Production")

    # --- SCENARIO 1: No Staging Model ---
    if not staging_model:
        print("⚠️ No model found in 'Staging'. Nothing to promote.")
        return

    # --- SCENARIO 2: Staging exists, but No Production Model ---
    if not production_model:
        print(f"✅ No 'Production' model found. Promoting Staging (v{staging_model.version}) directly to Production.")
        client.transition_model_version_stage(
            name=MODEL_NAME,
            version=staging_model.version,
            stage="Production",
            archive_existing_versions=False
        )
        return

    # --- SCENARIO 3: Both Exist - Compare Metrics ---
    print(f"⚖️ Comparing Staging (v{staging_model.version}) vs Production (v{production_model.version})...")

    # Fetch run data to get metrics
    staging_run = client.get_run(staging_model.run_id)
    production_run = client.get_run(production_model.run_id)

    staging_metric = staging_run.data.metrics.get(METRIC_KEY)
    production_metric = production_run.data.metrics.get(METRIC_KEY)

    if staging_metric is None or production_metric is None:
        print(f"❌ Could not find metric '{METRIC_KEY}' in one of the runs. Check your logging.")
        print(f"Trying {METRIC_KEY2}")
        staging_metric = staging_run.data.metrics.get(METRIC_KEY2)
        production_metric = production_run.data.metrics.get(METRIC_KEY2)
    else:
        print('❌❌❌❌ Both ways are note working ❌❌❌')


    print(f"   📉 Staging {METRIC_KEY}: {staging_metric:.4f}")
    print(f"   📉 Prod    {METRIC_KEY}: {production_metric:.4f}")

    # Determine which is better
    is_staging_better = False
    if LOWER_IS_BETTER:
        is_staging_better = staging_metric < production_metric
    else:
        is_staging_better = staging_metric > production_metric

    # Execute Promotion or Rejection
    if is_staging_better:
        print(f"🎉 Staging model is BETTER! Promoting v{staging_model.version} to Production.")
        
        # This command promotes Staging to Production AND automatically moves the old Production model to 'Archived'
        client.transition_model_version_stage(
            name=MODEL_NAME,
            version=staging_model.version,
            stage="Production",
            archive_existing_versions=True 
        )
        print(f"   Old Production (v{production_model.version}) has been Archived.")
        
    else:
        print(f"🚫 Staging model is WORSE (or equal). Keeping v{production_model.version} in Production.")
        print(f"   Archiving the rejected Staging model (v{staging_model.version}).")
        
        # Manually move the rejected Staging model to Archived
        client.transition_model_version_stage(
            name=MODEL_NAME,
            version=staging_model.version,
            stage="Archived"
        )

if __name__ == "__main__":
    promote_to_production()