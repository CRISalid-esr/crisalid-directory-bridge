import logging
from utils.config import get_env_variable
from airflow.decorators import task

logger = logging.getLogger(__name__)


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
    structure_branch = get_env_variable("LDAP_STRUCTURES_BRANCH")
    task_results = {}

    for row in source_data:
        task_results[f"supannCodeEntite={row['local_identifier']},{structure_branch}"] = {
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
            'identifiers': [
                {
                    'type': 'local',
                    'value': row['local_identifier']
                },
                {
                    'type': 'RNSR',
                    'value': row['RNSR']
                },
            ],
        }

    return task_results
