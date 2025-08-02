import logging
import fsspec
import yaml

from utils.exceptions import ConfigurationError

logger = logging.getLogger(__name__)


def load_yaml(path: str) -> dict:
    """
    Load and parse a YAML file from the given path.

    Args:
        path (str): The path to the YAML file (local or remote).

    Returns:
        dict: A dictionary containing the parsed YAML data.

    Raise:
        ConfigurationError: If the file does not exist.
    """
    try:
        with fsspec.open(path, "r", encoding="utf-8") as yaml_file:
            return yaml.safe_load(yaml_file)
    except FileNotFoundError as err:
        raise ConfigurationError(f"YAML file not found: {path}") from err
