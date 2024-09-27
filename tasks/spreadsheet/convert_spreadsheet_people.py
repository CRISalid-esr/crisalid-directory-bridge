import logging

from airflow.decorators import task

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

        task_results[f"{row[LOCAL_PERSON_IDENTIFIER]}"] = {
            'names': [
                {'last_names': [{'value': row['last_name'], 'language': 'fr'}],
                 'first_names': [{'value': row['first_name'], 'language': 'fr'}]}
            ],
            'identifiers': non_empty_identifiers,
            'memberships': [{'entity': row['main_laboratory_identifier']}]
        }

    return task_results
