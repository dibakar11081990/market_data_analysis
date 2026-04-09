import yaml
import os
from pathlib import Path

def load_config() -> dict:
    # Bringing configs
    config_path = Path(__file__).parent / "config.yml"

    try:
        with open(config_path, 'r') as file:
            return yaml.safe_load(file)

    except FileNotFoundError:
        raise FileNotFoundError(f"Config file not found at {config_path}")
    except yaml.YAMLError as e:
        raise ValueError(f"Error parsing YAML file: {e}")