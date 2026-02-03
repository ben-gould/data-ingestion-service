import yaml
from pathlib import Path

def load_config(config_path='config.yaml'):
    config_path = Path(config_path)

    if not config_path.is_absolute():
        config_path = Path(__file__).parent.parent.parent / config_path
        
    with open(config_path) as f:
        return yaml.safe_load(f)
    
    