import logging
import re

import yaml
from airflow.decorators import task

logger = logging.getLogger(__name__)


@task(task_id="convert_ldap_people_employment")
def convert_ldap_people_employment(ldap_results: dict[str, dict[str, str | dict]]):
    """

    Args:
        ldap_results (dict): A dict of ldap results with dn as key and entry as value

    Returns:
        dict: A dict of converted results with the "memberships" field populated
    """
    with open('./conf/employee_types.yml', 'r', encoding='UTF-8') as file:
        employee_types_yaml = yaml.safe_load(file)
    task_results = {}
    employments = []

    for dn, entry in ldap_results.items():
        related_establishment = entry.get('supannEtablissement', [])
        employee_type = entry.get('employeeType', [])
        len_establishment = len(related_establishment)
        len_employee_type = len(employee_type)

        if len_establishment and len_employee_type:
            formatted_establishment = [
                f"{match.group(1).lower()}-{item.split('}')[1]}"
                for item in related_establishment
                if (match := re.search(r'\{(.*?)\}', item))
            ]
            if len_establishment == len_employee_type:
                employments = [
                    {
                        "position": {
                            "title": inner_dict['label'],
                            "code": code
                        },
                        "entity_uid": entity_uid
                    }
                    for entity_uid, position_to_check in zip(formatted_establishment, employee_type)
                    for code, inner_dict in employee_types_yaml.items()
                    if position_to_check in inner_dict['local_values']
                ]
            else:
                # Both lengths are greater than 0 but not the same
                continue
        task_results[dn] = {"employments": employments}
    return task_results
