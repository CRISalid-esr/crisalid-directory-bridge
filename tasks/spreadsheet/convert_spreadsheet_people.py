import logging

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
    task_results['totologin'] = {'names': [
        {'last_names': [{'value': 'Dupont', 'language': 'fr'}],
         'first_names': [{'value': 'Benoit', 'language': 'fr'}]}],
        "identifiers": [{
            "type": "local",
            "value": "totologin"
        }, {
            "type": "orcid",
            "value": "0000-0000-0000-0000"
        }]}
    return task_results
