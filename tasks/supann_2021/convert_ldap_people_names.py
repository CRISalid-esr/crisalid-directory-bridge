import logging

from airflow.decorators import task

from utils.config import get_env_variable

logger = logging.getLogger(__name__)


@task(task_id="convert_ldap_people_names")
def convert_ldap_people_names(ldap_results: dict[str, dict[str, str | dict]],
                              config: dict[str, tuple[str, str]] = None) \
        -> dict[str, dict[str, str | dict]]:
    """Extract the 'name' field from an LDAP entry.

    Args:
        ldap_results (dict): A dict of ldap results with dn as key and entry as value
        config (dict): A configuration dict,
            not used in this function but required by the task signature

    Returns:
        dict: A dict of converted results with the "names" field populated
    """
    _ = config
    task_results = {}
    language = get_env_variable("LDAP_DEFAULT_LANGUAGE")
    for dn, entry in ldap_results.items():
        last_names = entry.get('sn')
        first_names = entry.get('givenName')
        if isinstance(last_names, list) and len(last_names) == 1:
            last_name = last_names[0]
        else:
            logger.error("Invalid last name for %s: %s", dn, last_names)
            last_name = None
        if isinstance(first_names, list) and len(first_names) == 1:
            first_name = first_names[0]
        else:
            logger.error("Invalid first name for %s: %s", dn, first_names)
            first_name = None
        task_results[dn] = {"names": [{
            "last_names": [{"value": last_name, "language": language}],
            "first_names": [{"value": first_name, "language": language}]}]
        }
    return task_results
