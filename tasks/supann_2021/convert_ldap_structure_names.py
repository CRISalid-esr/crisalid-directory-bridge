import logging

from airflow.decorators import task

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
        name = ldap_entry.get('eduorglegalname', ldap_entry.get('description', ''))
        task_results[dn] = {"names": [{"value": name, "language": language}]}

    return task_results
