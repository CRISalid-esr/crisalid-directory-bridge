import logging

from airflow.decorators import task

logger = logging.getLogger(__name__)

LOCAL_STRUCTURE_IDENTIFIER = 'tracking_id'

STRUCTURE_IDENTIFIERS = [LOCAL_STRUCTURE_IDENTIFIER, 'rnsr', 'ror']


@task(task_id="convert_spreadsheet_structures")
def convert_spreadsheet_structures(source_data: list[dict[str, str]]) -> dict[
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
            for identifier in STRUCTURE_IDENTIFIERS if row.get(identifier)
                                                       and row[identifier].strip()
        ]

        task_results[row[LOCAL_STRUCTURE_IDENTIFIER]] = {
            'names': [
                {
                    'value': row['name'],
                    'language': 'fr'
                },
            ],
            'acronym': row['acronym'] or None,
            'descriptions': [
                {
                    'value': row['description'],
                    'language': 'fr',
                }
            ],
            'contacts': [
                {
                    'type': 'postal_address',
                    'format': 'structured_physical_address',
                    'value': {
                        'country': 'France',
                        'zip_code': row['city_code'],
                        'city': row['city_name'],
                        'street': row['city_adress']
                    }
                }
            ],
            'identifiers': non_empty_identifiers,
        }

    return task_results
