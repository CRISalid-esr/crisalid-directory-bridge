# utils.py
import os

from dotenv import load_dotenv

APP_ENV = os.getenv("APP_ENV", "DEV")
load_dotenv(f".env.{APP_ENV.lower()}")


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
