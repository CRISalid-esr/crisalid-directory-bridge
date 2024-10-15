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

        if len(related_establishment) > len(employee_type):
            employee_type += [''] * (len(related_establishment) - len(employee_type))

        formatted_establishment = [
            f"uai-{match.group(1)}"
            for item in related_establishment
            # Regular expression match example: {UAI}0000000Z
            if (match := re.search(r'^\{UAI\}(\d{7}[A-Z])$', item))
        ]

        for entity_uid, position_to_check in zip(formatted_establishment, employee_type):
            match = next(
                ((code, inner_dict['label']) for code, inner_dict in employee_types_yaml.items()
                  if position_to_check in inner_dict['local_values']),
                ('', '')
            )

            employment = {
                "position": {
                    "title": match[1],
                    "code": match[0]
                },
                "entity_uid": entity_uid
            }
            employments.append(employment)

        task_results[dn] = {"employments": employments}
    return task_results
