from airflow.decorators import task

from utils.config import get_env_variable


@task(task_id="convert_ldap_structure_descriptions")
def convert_ldap_structure_descriptions(ldap_results: dict[str, dict[str, str | dict]]) \
        -> dict[str, str]:
    """
    Extract the 'description' field from a dict of LDAP entries.

    Args:
        ldap_results (dict): A dict of LDAP results with dn as key and entry as value.

    Returns:
        dict: A dict of descriptions with dn as key and description as value.
    """
    task_results = {}
    language = get_env_variable("LDAP_DEFAULT_LANGUAGE")
    for dn, ldap_entry in ldap_results.items():
        assert ldap_entry is not None, f"LDAP entry is None for dn: {dn}"
        descriptions = ldap_entry.get('description', '')
        if isinstance(descriptions, list) and len(descriptions) > 0:
            task_results[dn] = {"descriptions": [{"value": descriptions[0], "language": language}]}
        else:
            task_results[dn] = {"descriptions": []}

    return task_results
