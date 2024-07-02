from airflow.decorators import task


@task
def convert_ldap_structure_acronym_task(ldap_result: dict[str, str | dict]) -> str:
    """Extract the 'acroynm' field from an LDAP entry.

    Args:
        ldap_result (dict): An LDAP result with "dn" and "entry" fields.

    Returns:
        str: The 'acronym' field.
    """
    ldap_entry = ldap_result.get('entry', None)
    assert ldap_entry is not None, "LDAP entry is None"
    return ldap_entry.get('acronym', '')
