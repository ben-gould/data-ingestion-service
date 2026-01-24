import yaml
from pathlib import Path

def load_config(config_path='/Users/bengould/Documents/Projects/data-ingestion-service/config.yaml'):
    with open(config_path) as f:
        return yaml.safe_load(f)