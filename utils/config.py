# utils.py
import logging
import os

from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Search for generic and environment-specific .env files
APP_ENV = os.getenv("APP_ENV", "DEV")
for suffix in ["", f".{APP_ENV.lower()}"]:
    env_file_name = f".env{suffix}"
    if not os.path.exists(env_file_name):
        logger.warning("%s env file not found", env_file_name)
    else:
        logger.info("Loading env file: %s", env_file_name)
        load_dotenv(env_file_name, verbose=True)


def get_env_variable(var_name: str, default_value: str = None) -> str:
    """"
    Get an environment variable or raise an error if it is not set and no default value is provided.
    Args:
        var_name (str): The name of the environment variable.
        default_value (str): The default value to use if the environment variable is not set.
    """
    value = os.getenv(var_name) or os.getenv(var_name, default_value)
    if value is None:
        raise ValueError(f"Environment variable {var_name} not set")
    return value
