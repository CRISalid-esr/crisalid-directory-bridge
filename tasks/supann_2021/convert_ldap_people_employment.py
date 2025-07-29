import logging
import re

from airflow.decorators import task

from utils.yaml_loader import get_position_by_local_value

logger = logging.getLogger(__name__)


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
        "entity_uid": f"UAI-{entity_id}",
    }
    return employment


def _convert_with_employee_type(
        employee_entry: dict[(str, list[str]), (str, list[str])]
) -> list[dict]:
    employments = []
    institutions = employee_entry.get('supannEtablissement', [])
    institution_count = len(institutions)
    if institution_count == 0:
        return employments

    employee_types = employee_entry.get('employeeType', [])
    employee_type_count = len(employee_types)

    # Fill the missing employee types with None
    if institution_count > employee_type_count:
        employee_types += [None] * (institution_count - employee_type_count)

    # Fill the missing institutions with the first one
    if institution_count < employee_type_count:
        if institution_count == 1:
            institutions.extend(
                [institutions[0]] * (employee_type_count - institution_count))
        else:
            employee_types = [None] * institution_count

    formatted_institution = [
        f"{match.group(1)}"
        for item in institutions
        # Regular expression match example: {UAI}0000000Z
        if (match := re.search(r'^\{UAI\}(\d{7}[A-Z])$', item))
    ]
    for entity_uid, employee_type_to_check in zip(formatted_institution, employee_types):

        position = None
        if employee_type_to_check:
            position = get_position_by_local_value(employee_type_to_check)
            if not position:
                logger.warning(
                    "Employee type '%s' not found in YAML local_values",
                    employee_type_to_check
                )

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
        employments = _convert_with_employee_type(entry)
        task_results[dn] = {"employments": employments}
    return task_results
