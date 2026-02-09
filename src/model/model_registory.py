



import json
import mlflow
import logging
import os
import dagshub
from pathlib import Path
from mlflow.tracking import MlflowClient
import warnings


# Set up DagsHub credentials for MLflow tracking
# dagshub_token = os.getenv("CAPSTONE_TEST")
# if not dagshub_token:
#     raise EnvironmentError("CAPSTONE_TEST environment variable is not set")

# os.environ["MLFLOW_TRACKING_USERNAME"] = dagshub_token
# os.environ["MLFLOW_TRACKING_PASSWORD"] = dagshub_token

# dagshub_url = "https://dagshub.com"
# repo_owner = "wadoodabdulwadood122010" 
# repo_name = "Temperature-prediction-Mlops-project"
# # Set up MLflow tracking URI
# mlflow.set_tracking_uri(f'{dagshub_url}/{repo_owner}/{repo_name}.mlflow')
# Suppress warnings
warnings.simplefilter("ignore", UserWarning)
warnings.filterwarnings("ignore")

# --- CONFIGURATION ---
# Define Paths relative to the project root
BASE_DIR = Path(__file__).resolve().parent.parent.parent









METRICS_PATH = BASE_DIR / "reports/metrics.json"

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# -------------------------------------------------------------------------------------
# DAGSHUB & MLFLOW SETUP
# -------------------------------------------------------------------------------------
REPO_OWNER = "wadoodabdulwadood122010"
REPO_NAME = "Temperature-prediction-Mlops-project"

# Initialize DagsHub (Auto-configures MLflow tracking)
dagshub.init(repo_owner=REPO_OWNER, repo_name=REPO_NAME, mlflow=True)
mlflow.set_tracking_uri(f'https://dagshub.com/{REPO_OWNER}/{REPO_NAME}.mlflow')

def load_model_info(file_path: Path) -> dict:
    """Load the model info from a JSON file."""
    try:
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
            
        with open(file_path, 'r') as file:
            model_info = json.load(file)
        
        logging.info(f'Model info loaded from {file_path}')
        return model_info
        
    except Exception as e:
        logging.error(f'Error loading model info: {e}')
        raise

def register_model(model_name: str, model_info: dict):
    """Register the model to the MLflow Model Registry."""
    try:
        run_id = model_info['run_id']
        # Default to "model" if 'model_path' is missing in JSON
        artifact_path = model_info.get('model_path', 'model')
        
        model_uri = f"runs:/{run_id}/{artifact_path}"
        
        print(f"🚀 Registering model from URI: {model_uri}")
        
        # Register the model
        model_version = mlflow.register_model(model_uri, model_name)
        
        # Transition the model to "Staging"
        client = MlflowClient()
        client.transition_model_version_stage(
            name=model_name,
            version=model_version.version,
            stage="Staging"
        )
        
        logging.info(f'✅ Model {model_name} (Version {model_version.version}) registered and moved to Staging.')
        
    except Exception as e:
        logging.error(f'❌ Error during model registration: {e}')
        print("\n⚠️  TIP: If you get a 'RESOURCE_DOES_NOT_EXIST' error, ensure you logged the model in model_evaluation.py using mlflow.sklearn.log_model()")
        raise

def main():
    try:
        model_info = load_model_info(METRICS_PATH)
        
        model_name = "Pakistan-Weather-Forecast"
        register_model(model_name, model_info)
        
    except Exception as e:
        logging.error(f'Failed to complete model registration: {e}')

if __name__ == '__main__':
    main()
