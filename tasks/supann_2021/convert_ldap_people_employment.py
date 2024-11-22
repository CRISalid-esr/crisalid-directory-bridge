import fsspec
import logging
import re

import yaml
from airflow.decorators import task

from utils.config import get_env_variable
from utils.exceptions import YamlParseError

logger = logging.getLogger(__name__)
yaml_path = get_env_variable('YAML_EMPLOYEE_TYPE_PATH')
employee_types_yaml = None # pylint: disable=invalid-name

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


def _get_position_code_and_label(
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
            if corps["local_values"] and employee_type_to_check in corps["local_values"]:
                position = (corps["corps"], corps["label"])
                return position

        logger.warning(
            "Employee type '%s' from field type '%s' not found in '%s'",
            employee_type_to_check, field_type, yaml_path
        )
    return position


def _create_employment(
        entity_id: str,
        position: tuple[str, str] | None
) -> dict[str, dict[str, str] | dict]:
    """
    Args:
        entity_id(str): The entity where the employee is located
        position(tuple[str, str] | None): A tuple with the code and the label of the position
    Returns:
        A dict containing the position code and label of the employee in the corresponding entity
    """
    employment = {
        "position": {} if not position else {
            "title": position[1],
            "code": position[0]
        },
        "entity_uid": entity_id
    }
    return employment


def _convert_with_employeetype(
        employee_entry: dict[(str, list[str]), (str, list[str])]
) -> list[dict]:
    employments = []
    establishments = employee_entry.get('supannEtablissement', [])
    establishment_count = len(establishments)
    if establishment_count == 0:
        return employments
    employee_types = employee_entry.get('employeeType', [])
    employee_type_count = len(employee_types)

    if establishment_count > employee_type_count:
        employee_types += [None] * (establishment_count - employee_type_count)

    if establishment_count < employee_type_count:
        if establishment_count == 1:
            establishments.extend(
                [establishments[0]] * (employee_type_count - establishment_count))
        else:
            employee_types = [None] * establishment_count

    formatted_establishment = [
        f"uai-{match.group(1)}"
        for item in establishments
        # Regular expression match example: {UAI}0000000Z
        if (match := re.search(r'^\{UAI\}(\d{7}[A-Z])$', item))
    ]
    for entity_uid, employee_type_to_check in zip(formatted_establishment, employee_types):
        position = _get_position_code_and_label(employee_type_to_check, 'local_values')
        employments.append(_create_employment(entity_uid, position))
    return employments


@task(task_id="convert_ldap_people_employment")
def convert_ldap_people_employment(
        ldap_results: dict[str, dict[str, str | dict]]
) -> dict[str, dict[str, list[dict]]]:
    """
    Args:
        ldap_results (dict): A dict of ldap results with dn as key and entry as value
    Returns:
        dict: A dict of converted results with the "memberships" field populated
    """
    task_results = {}

    for dn, entry in ldap_results.items():
        employments = _convert_with_employeetype(entry)
        task_results[dn] = {"employments": employments}
    return task_results
