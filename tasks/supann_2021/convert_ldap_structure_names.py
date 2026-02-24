import logging

from airflow.sdk import task

from utils.config import get_env_variable

logger = logging.getLogger(__name__)


@task(task_id="convert_ldap_structure_names")
def convert_ldap_structure_names(ldap_results: dict[str, dict[str, str | dict]]) \
        -> dict[str, list[dict]]:
    """
    Extract the 'name' field from a dict of LDAP entries.

    Args:
        ldap_results (dict): A dict of LDAP results with dn as key and entry as value.

    Returns:
        dict: A dict of names with dn as key and a
        list containing dictionaries with 'value' and 'language'
    """
    task_results = {}
    language = get_env_variable("LDAP_DEFAULT_LANGUAGE")
    for dn, ldap_entry in ldap_results.items():
        assert ldap_entry is not None, f"LDAP entry is None for dn: {dn}"
        logger.error("LDAP entry: %s", ldap_entry)
        name = ldap_entry.get('eduorglegalname', ldap_entry.get('ou', ldap_entry.get(
            'description', [])))
        if isinstance(name, list) and len(name) > 0:
            name = name[0]
        else:
            logger.error("Invalid name for %s: %s", dn, name)
            name = None
        task_results[dn] = {"names": [{"value": name, "language": language}]}

    return task_results
