from airflow.decorators import task


@task(task_id="convert_ldap_structure_acronyms")
def convert_ldap_structure_acronyms(ldap_results: dict[str, dict[str, str | dict]]) \
        -> dict[str, str]:
    """
    Extract the 'acronym' field from a dict of LDAP entries.

    Args:
        ldap_results (dict): A dict of LDAP results with dn as key and entry as value.

    Returns:
        dict: A dict of acronyms with dn as key and acronym as value.
    """
    task_results = {}
    for dn, ldap_entry in ldap_results.items():
        assert ldap_entry is not None, f"LDAP entry is None for dn: {dn}"
        acronyms = ldap_entry.get('acronym', '')
        if isinstance(acronyms, list) and len(acronyms) > 0:
            acronym = acronyms[0]
        else:
            acronym = None
        task_results[dn] = {"acronym": acronym}

    return task_results
