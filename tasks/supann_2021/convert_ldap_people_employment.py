import re

from airflow.decorators import task

from utils.employee_types import get_position_code_and_label


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
        position = get_position_code_and_label(employee_type_to_check, 'local_values')
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
