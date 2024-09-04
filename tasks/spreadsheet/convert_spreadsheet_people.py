import logging
from utils.config import get_env_variable
from airflow.decorators import task

logger = logging.getLogger(__name__)


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
        task_results[f"uid={row['local_identifier']}"] = {
            'names': [
                {'last_names': [{'value': row['last_name'], 'language': 'fr'}],
                 'first_names': [{'value': row['first_name'], 'language': 'fr'}]}
            ],
            'identifiers': [
                {'type': 'local', 'value': row['local_identifier']},
                {'type': 'orcid', 'value': row['orcid']},
                {'type': 'idref', 'value': row['idref']},
            ],
            'memberships': [{'entity': row['main_laboratory_identifier']}]
        }

    return task_results
