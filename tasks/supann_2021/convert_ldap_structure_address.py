from airflow.decorators import task


@task
def convert_ldap_structure_address_task(ldap_result: dict[str, str | dict]) -> str:
    """Extract the 'address' field from an LDAP entry.

    Args:
        ldap_result (dict): An LDAP result with "dn" and "entry" fields.

    Returns:
        str: The 'address' field.
    """
    ldap_entry = ldap_result.get('entry', None)
    assert ldap_entry is not None, "LDAP entry is None"
    return ldap_entry.get('postalAddress', '')
