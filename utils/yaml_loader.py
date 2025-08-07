import logging

import fsspec
import yaml
from airflow.decorators import task

from utils.exceptions import ConfigurationError

logger = logging.getLogger(__name__)


@task(task_id='load_yaml')
def load_yaml(path: str) -> dict:
    """
    Load and parse a YAML file from the given path.

    Args:
        path (str): The path to the YAML file (local or remote).

    Returns:
        dict: A dictionary containing the parsed YAML data.

    Raise:
        ConfigurationError: If there is an error parsing the YAML file or if the file is not found.
    """
    try:
        with fsspec.open(path, "r", encoding="utf-8") as yaml_file:
            return yaml.safe_load(yaml_file)
    except yaml.YAMLError as err:
        raise ConfigurationError(f"Error parsing YAML file: {path}") from err
    except FileNotFoundError as err:
        raise ConfigurationError(f"YAML file not found: {path}") from err
