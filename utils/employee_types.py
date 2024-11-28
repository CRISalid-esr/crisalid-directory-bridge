import logging

import fsspec
import yaml

from utils.config import get_env_variable
from utils.exceptions import YamlParseError

yaml_path = get_env_variable('YAML_EMPLOYEE_TYPE_PATH')
employee_types_yaml = None  # pylint: disable=invalid-name

logger = logging.getLogger(__name__)


def _get_employee_types_yaml():
    global employee_types_yaml  # pylint: disable=global-statement
    if employee_types_yaml is not None:
        return employee_types_yaml
    try:
        with fsspec.open(yaml_path, 'r', encoding='UTF-8') as file:
            employee_types_yaml = yaml.safe_load(file)
    except (FileNotFoundError, ValueError, yaml.YAMLError) as error:
        raise YamlParseError(f"Error while reading the YAML: {str(error)}") from error

    return employee_types_yaml


def get_position_code_and_label(
        employee_type_to_check: str | None,
        field_type: str
) -> tuple[str, str] | None:
    """
    Args:
        employee_type_to_check(str): An employee type value who come from LDAP
        field_type(str): The type of field to compare with employee_type_to_check
    Returns:
        tuple[str, str] | None: A tuple with the code and the label of the position if the
        employee type is known. Else None
    """
    position = None
    if employee_type_to_check is not None:
        corps_list = [
            corps
            for status in _get_employee_types_yaml().values()
            for category in status.values()
            for corps in category
        ]

        for corps in corps_list:
            if corps[field_type] and employee_type_to_check in corps[field_type]:
                position = (corps["corps"], corps["label"])
                return position

        logger.warning(
            "Employee type '%s' from field type '%s' not found in '%s'",
            employee_type_to_check, field_type, yaml_path
        )
    return position
