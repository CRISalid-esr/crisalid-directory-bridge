import logging

from airflow.decorators import task

from utils.employee_types import get_position_code_and_label

logger = logging.getLogger(__name__)

LOCAL_PERSON_IDENTIFIER = 'local'

PERSON_IDENTIFIERS = [LOCAL_PERSON_IDENTIFIER,
                      'id_hal_i',
                      'id_hal_s',
                      'orcid',
                      'idref',
                      'scopus_eid']


@task(task_id="convert_spreadsheet_people")
def convert_spreadsheet_people(source_data: list[dict[str, str]]) -> dict[
    str, dict[str, str | dict]
]:
    """
    Extract the 'identifier' field from a dict of ldap entries

    Args:
        source_data (dict): A dict of ldap results with dn as key and entry as value

    Returns:
        dict: A dict of converted results with the "identifiers" field populated
    """
    task_results = {}

    for row in source_data:
        non_empty_identifiers = [
            {'type': identifier, 'value': row[identifier]}
            for identifier in PERSON_IDENTIFIERS if row.get(identifier) and row[identifier].strip()
        ]

        if len(non_empty_identifiers) == 0:
            logger.warning("No identifiers for row: %s", row)

        position_code = row.get('position', logger.warning("Position is empty %s", row))
        position = get_position_code_and_label(position_code, "corps")

        task_results[f"{row[LOCAL_PERSON_IDENTIFIER]}"] = {
            'names': [
                {'last_names': [{'value': row['last_name'], 'language': 'fr'}],
                 'first_names': [{'value': row['first_name'], 'language': 'fr'}]}
            ],
            'identifiers': non_empty_identifiers,
            'memberships': [{'entity': row['main_laboratory_identifier']}],
            'employments': [
                {
                    'position': {} if not position else {
                        "title": position[1],
                        "code": position[0]
                    },
                    'entity_uid': row.get(
                        'institution_identifier',
                        logger.warning("entity_uid is empty %s", row))
                }
            ]
        }

    return task_results
