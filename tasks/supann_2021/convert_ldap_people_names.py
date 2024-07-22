import logging

from airflow.decorators import task

from utils.config import get_env_variable

logger = logging.getLogger(__name__)


@task(task_id="convert_ldap_people_names")
def convert_ldap_people_names(ldap_results: dict[str, dict[str, str | dict]]) \
        -> dict[str, dict[str, str | dict]]:
    """Extract the 'name' field from an LDAP entry.

    Args:
        ldap_results (dict): A dict of ldap results with dn as key and entry as value

    Returns:
        dict: A dict of converted results with the "names" field populated
    """
    task_results = {}
    language = get_env_variable("LDAP_DEFAULT_LANGUAGE")
    for dn, entry in ldap_results.items():
        last_name = entry.get('sn')
        first_name = entry.get('givenName')
        task_results[dn] = {"names": [{
            "last_names": [{"value": last_name, "language": language}],
            "first_names": [{"value": first_name, "language": language}]}]
        }
    return task_results
