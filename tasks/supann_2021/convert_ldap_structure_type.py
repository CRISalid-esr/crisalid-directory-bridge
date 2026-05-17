import os

from airflow.sdk import task


@task(task_id="convert_ldap_structure_type")
def convert_ldap_structure_type(ldap_results: dict) -> dict:
    """
    Return generic_type and main_mission for each LDAP structure entry.

    generic_type is read from LDAP_STRUCTURE_GENERIC_TYPE (default: 'unit').
    main_mission is hardcoded to 'research' — only used by IKG for Unit subtypes.

    Args:
        ldap_results (dict): A dict of LDAP results with dn as key.

    Returns:
        dict: A dict with dn as key and {"generic_type": str, "main_mission": str} as value.
    """
    generic_type = os.getenv("LDAP_STRUCTURE_GENERIC_TYPE", "unit")
    return {dn: {"generic_type": generic_type, "main_mission": "research"} for dn in ldap_results}
