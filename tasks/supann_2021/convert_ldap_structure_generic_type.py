import os

from airflow.sdk import task


@task(task_id="convert_ldap_structure_generic_type")
def convert_ldap_structure_generic_type(ldap_results: dict) -> dict:
    """
    Return a generic_type for each LDAP structure entry.

    The value is read from the LDAP_STRUCTURE_GENERIC_TYPE environment variable,
    defaulting to 'unit' (covers most French university LDAP-sourced structures).

    Args:
        ldap_results (dict): A dict of LDAP results with dn as key.

    Returns:
        dict: A dict with dn as key and {"generic_type": str} as value.
    """
    generic_type = os.getenv("LDAP_STRUCTURE_GENERIC_TYPE", "unit")
    return {dn: {"generic_type": generic_type} for dn in ldap_results}
