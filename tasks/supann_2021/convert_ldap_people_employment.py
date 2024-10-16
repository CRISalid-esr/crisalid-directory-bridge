import logging
import re

import yaml
from airflow.decorators import task

from utils.config import get_env_variable
from utils.exceptions import YamlParseError

logger = logging.getLogger(__name__)


def _get_position_code_and_label(
        employee_type_to_check: str,
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
    yaml_path = get_env_variable('YAML_EMPLOYEE_TYPE_PATH')
    try:
        with open(yaml_path, 'r', encoding='UTF-8') as file:
            employee_types_yaml = yaml.safe_load(file)
    except (FileNotFoundError, ValueError, yaml.YAMLError) as error:
        raise YamlParseError(f"Error while reading the yaml: {str(error)}") from error

    position = next(
        ((code, inner_dict['label']) for code, inner_dict in employee_types_yaml.items()
         if employee_type_to_check in inner_dict[field_type]),
        None
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


def _convert_with_supanempprofil(
        list_dict_profil: list[dict]
) -> list[dict]:
    employments = []
    for profil in list_dict_profil:
        entity_uid = f"uai-{profil['etab'].removeprefix('{UAI}')}"
        position = None
        corps = profil.get('corps', None)
        if corps:
            corps = profil['corps'].removeprefix('{NCORPS}')
            position = _get_position_code_and_label(corps, 'corps')
        employments.append(_create_employment(entity_uid, position))
    return employments


def _convert_with_employeetype(
        employee_entry: dict[list[str], list[str]]
) -> list[dict]:
    employments = []
    related_establishment = employee_entry.get('supannEtablissement', [])
    establishment_count = len(related_establishment)
    employee_type = employee_entry.get('employeeType', [])
    employee_type_count = len(employee_type)

    if establishment_count > employee_type_count:
        employee_type += [None] * (establishment_count - employee_type_count)

    if establishment_count == 1 < employee_type_count:
        related_establishment.extend(
            [related_establishment[0]] * (employee_type_count - establishment_count))

    # NOTE:If we have multiple establishments and multiple type,
    # but we have more type than establishments, we can't rely on the lists to make correspondances
    if 1 < establishment_count < employee_type_count:
        employee_type = [None] * establishment_count

    formatted_establishment = [
        f"uai-{match.group(1)}"
        for item in related_establishment
        # Regular expression match example: {UAI}0000000Z
        if (match := re.search(r'^\{UAI\}(\d{7}[A-Z])$', item))
    ]
    for entity_uid, employee_type_to_check in zip(formatted_establishment, employee_type):
        position = _get_position_code_and_label(employee_type_to_check, 'local_values')
        employments.append(_create_employment(entity_uid, position))
    return employments


@task(task_id="convert_ldap_people_employment")
def convert_ldap_people_employment(
        ldap_results: dict[str, dict[str, str | dict]]
) -> dict[dict[list[dict[str, str | dict]]]]:
    """
    Args:
        ldap_results (dict): A dict of ldap results with dn as key and entry as value
    Returns:
        dict: A dict of converted results with the "memberships" field populated
    """
    task_results = {}

    for dn, entry in ldap_results.items():
        emp_profil = entry.get('supannEmpProfil', [])
        list_dict_profil = [
            dict(item.split('=') for item in profil.strip('[]').split(']['))
            for profil in emp_profil
        ]
        if len(list_dict_profil) > 0:
            employments = _convert_with_supanempprofil(list_dict_profil)
        else:
            employments = _convert_with_employeetype(entry)

        task_results[dn] = {"employments": employments}
    return task_results
