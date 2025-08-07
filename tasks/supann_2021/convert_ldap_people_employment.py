import logging
import re

from airflow.decorators import task

logger = logging.getLogger(__name__)

def _create_employment(
        entity_id: str,
        position: tuple[str, str] | None
) -> dict[str, dict[str, str] | dict]:
    """
    Build an employment record for a given entity and position.

    Args:
        entity_id (str): The unique identifier of the entity where the employee works.
        position (tuple[str, str] | None): A tuple containing the position code (str)
            and label (str). If None, no position information is included.

    Returns:
        dict: A dictionary containing the entity UID and the employee's position data.
            Example:
            {
                "position": {"title": "<label>", "code": "<code>"},
                "entity_uid": "UAI-<entity_id>"
            }
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
        employee_entry: dict[(str, list[str]), (str, list[str])],
        local_value_position_dict: dict[str, tuple[str, str]],
) -> list[dict]:
    """
    Convert LDAP employee entry data into a list of employment records.

    This function maps LDAP fields (employee type and institution) to structured
    employment dictionaries, using a local mapping of employee types to position data.

    Args:
        employee_entry (dict): A dictionary containing LDAP employee attributes,
            typically including 'supannEtablissement' (institutions) and 'employeeType'.
        local_value_position_dict (dict): A mapping of employee type codes (str) to
            tuples of (position_code, position_label).

    Returns:
        list[dict]: A list of employment records formatted for further processing.
    """
    employments = []
    institutions = employee_entry.get('supannEtablissement', [])
    institution_count = len(institutions)
    if institution_count == 0:
        return employments

    employee_types = employee_entry.get('employeeType', [])
    employee_type_count = len(employee_types)

    # Fill missing employee types with None
    if institution_count > employee_type_count:
        employee_types += [None] * (institution_count - employee_type_count)

    # Adjust if there are more employee types than institutions
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
        if employee_type_to_check is not None:
            position = local_value_position_dict.get(employee_type_to_check or "")
            if not position:
                logger.warning(
                    "Employee type '%s' not found in YAML local_values",
                    employee_type_to_check
                )

        employments.append(_create_employment(entity_uid, position))

    return employments


@task(task_id="convert_ldap_people_employment")
def convert_ldap_people_employment(
        ldap_results: dict[str, dict[str, str | dict]],
        config: dict[str, tuple[str, str]] = None,
) -> dict[str, dict[str, list[dict]]]:
    """
    Convert LDAP results into a dictionary of employment information.

    This Airflow task processes LDAP entries, extracts institution and employee
    type data, and formats them into a structured "employments" field.

    Args:
        ldap_results (dict): A dictionary of LDAP entries keyed by DN (distinguished name),
            where each value is another dictionary of attributes.
        config (dict): A mapping of employee type codes to tuples
            of (position_code, position_label).

    Returns:
        dict: A dictionary keyed by DN, where each value contains an "employments" list.
            Example:
            {
                "uid=jdoe,ou=people,dc=example,dc=com": {
                    "employments": [
                        {"position": {"title": "...", "code": "..."}, "entity_uid": "UAI-XXXXXXX"}
                    ]
                }
            }
    """
    task_results = {}

    for dn, entry in ldap_results.items():
        employments = _convert_with_employee_type(entry, config)
        task_results[dn] = {"employments": employments}
    return task_results
