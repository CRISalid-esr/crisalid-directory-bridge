import re

from airflow.sdk import task

from utils.config import get_env_variable


@task(task_id="convert_ldap_structure_short_labels")
def convert_ldap_structure_short_labels(ldap_results: dict[str, dict[str, str | dict]]) \
        -> dict[str, dict]:
    """
    Extract the acronym from a dict of LDAP entries.

    Args:
        ldap_results (dict): A dict of LDAP results with dn as key and entry as value.

    Returns:
        dict: A dict with dn as key and short_labels list as value.
    """
    task_results = {}
    language = get_env_variable("LDAP_DEFAULT_LANGUAGE")
    for dn, ldap_entry in ldap_results.items():
        assert ldap_entry is not None, f"LDAP entry is None for dn: {dn}"
        short_label = None
        ou = ldap_entry.get('ou', [])
        if isinstance(ou, list) and len(ou) > 0:
            short_label = ou[0]
        elif isinstance(ou, str) and ou:
            short_label = ou
        if short_label is None:
            descriptions = ldap_entry.get('description', [])
            if isinstance(descriptions, list) and len(descriptions) > 0:
                match = re.match(r"([^:]*)\s:", descriptions[0])
                if match:
                    short_label = match.group(1)

        if short_label:
            task_results[dn] = {"short_labels": [{"value": short_label, "language": language}]}
        else:
            task_results[dn] = {"short_labels": []}

    return task_results
