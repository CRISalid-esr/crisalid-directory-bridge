import logging
from functools import lru_cache

import fsspec
import yaml
from utils.exceptions import YamlParseError
from utils.config import get_env_variable

logger = logging.getLogger(__name__)

employee_type_index: dict[str, tuple[str, str]] = {}


@lru_cache(maxsize=1)
def load_employee_types() -> dict:
    """
    Load and cache the employee types YAML file.

    Returns:
        dict: Parsed YAML content containing employee type groups.
    """
    yaml_path = get_env_variable("YAML_EMPLOYEE_TYPE_PATH")
    logger.debug("Loading employee types YAML from %s", yaml_path)

    try:
        with fsspec.open(yaml_path, "r", encoding="utf-8") as yaml_file:
            employee_types = yaml.safe_load(yaml_file)
    except (FileNotFoundError, yaml.YAMLError, ValueError) as error:
        raise YamlParseError(f"Failed to load YAML file '{yaml_path}': {error}") from error

    _build_employee_type_index(employee_types)
    return employee_types


def _build_employee_type_index(employee_types: dict) -> None:
    """
    Populate the single lookup dictionary with both corps codes and local values.

    Args:
        employee_types (dict): Parsed YAML data structure.
    """
    for group_content in employee_types.values():
        for positions_list in group_content.values():
            for position_entry in positions_list:
                corps_code = position_entry.get("corps")
                label = position_entry.get("label", "")

                if corps_code:
                    employee_type_index[corps_code] = (corps_code, label)

                for local_val in position_entry.get("local_values") or []:
                    employee_type_index[local_val] = (corps_code, label)


def get_position_by_code(position_code: str) -> tuple[str, str]:
    """
    Retrieve the corps code and label for a given corps code.

    Args:
        position_code (str): The corps code to look up.

    Returns:
        tuple[str, str]: A tuple (corps_code, label) if found.

    Raises:
        ValueError: If the corps code is not found in the YAML.
    """
    if not employee_type_index:
        load_employee_types()

    if position_code in employee_type_index:
        return employee_type_index[position_code]

    raise ValueError(f"Position code '{position_code}' not found in YAML.")


def get_position_by_local_value(local_value: str) -> tuple[str, str] | None:
    """
    Retrieve the corps code and label for a given local value.

    Args:
        local_value (str): The local value to look up.

    Returns:
        tuple[str, str] | None: (corps_code, label) if the local value exists,
        otherwise None. Logs a warning if not found.
    """
    if not employee_type_index:
        load_employee_types()

    employee_type = employee_type_index.get(local_value)
    if employee_type is None:
        logger.warning("Local value '%s' not found in employee types YAML index", local_value)
    return employee_type
