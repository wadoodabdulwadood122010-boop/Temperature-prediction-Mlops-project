import mlflow
from mlflow.tracking import MlflowClient
import os
import sys

# --- CONFIGURATION ---
DAGSHUB_REPO_OWNER = "wadoodabdulwadood122010"
DAGSHUB_REPO_NAME = "Temperature-prediction-Mlops-project"
MODEL_NAME = "Pakistan-Weather-Forecast"

# We now support checking both new and old keys
METRIC_KEY_NEW = "eval_mae"
METRIC_KEY_OLD = "mae" 
LOWER_IS_BETTER = True 

# --- AUTHENTICATION ---
dagshub_token = os.getenv("DAGSHUB_TOCKEN")
if not dagshub_token:
    raise EnvironmentError("DAGSHUB_TOCKEN environment variable is not set")

os.environ["MLFLOW_TRACKING_USERNAME"] = dagshub_token
os.environ["MLFLOW_TRACKING_PASSWORD"] = dagshub_token

dagshub_url = "https://dagshub.com"
mlflow.set_tracking_uri(f'{dagshub_url}/{DAGSHUB_REPO_OWNER}/{DAGSHUB_REPO_NAME}.mlflow')

def get_latest_version(client, model_name, stage):
    try:
        versions = client.get_latest_versions(model_name, stages=[stage])
    except Exception:
        return None

    if not versions:
        return None
    return max(versions, key=lambda x: int(x.version))

def get_metric_safe(run, key_primary, key_secondary):
    """
    Tries to fetch the metric using the primary key. 
    If not found, tries the secondary (legacy) key.
    """
    val = run.data.metrics.get(key_primary)
    if val is not None:
        return val
    return run.data.metrics.get(key_secondary)

def promote_to_production():
    print(f"🚀 Checking models for: {MODEL_NAME}")
    
    client = MlflowClient()

    # 1. Fetch Models
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
            archive_existing_versions=True
        )
        print("🎉 Promotion Complete.")
        return

    # --- SCENARIO 3: Both Exist - Compare Metrics ---
    print(f"⚖️ Comparing Staging (v{staging_model.version}) vs Production (v{production_model.version})...")

    try:
        staging_run = client.get_run(staging_model.run_id)
        production_run = client.get_run(production_model.run_id)
    except Exception as e:
        print(f"❌ Error fetching run data: {e}")
        return

    # Use the helper function to handle both 'eval_mae' and 'mae'
    staging_metric = get_metric_safe(staging_run, METRIC_KEY_NEW, METRIC_KEY_OLD)
    production_metric = get_metric_safe(production_run, METRIC_KEY_NEW, METRIC_KEY_OLD)

    if staging_metric is None or production_metric is None:
        print(f"❌ Could not find metric '{METRIC_KEY_NEW}' or '{METRIC_KEY_OLD}' in one of the runs.")
        print(f"   Staging (v{staging_model.version}) metrics: {staging_run.data.metrics.keys()}")
        print(f"   Prod (v{production_model.version}) metrics: {production_run.data.metrics.keys()}")
        sys.exit(1)

    print(f"   📉 Staging MAE: {staging_metric:.4f}")
    print(f"   📉 Prod    MAE: {production_metric:.4f}")

    # Determine which is better
    is_staging_better = False
    if LOWER_IS_BETTER:
        is_staging_better = staging_metric < production_metric
    else:
        is_staging_better = staging_metric > production_metric

    if is_staging_better:
        print(f"🎉 Staging model is BETTER! Promoting v{staging_model.version} to Production.")
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
        client.transition_model_version_stage(
            name=MODEL_NAME,
            version=staging_model.version,
            stage="Archived"
        )

if __name__ == "__main__":
    promote_to_production()