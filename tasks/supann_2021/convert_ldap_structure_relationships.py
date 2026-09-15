from airflow.sdk import task


@task(task_id="convert_ldap_structure_relationships")
def convert_ldap_structure_relationships(ldap_results: dict) -> dict:
    """
    Build relationships from LDAP structure entries.

    If supannCodeEntiteParent is present with value XYZ, produces a
    part_of relationship to local-XYZ. Otherwise produces an empty list.

    Args:
        ldap_results (dict): A dict of LDAP results with dn as key.

    Returns:
        dict: A dict with dn as key and {"relationships": list} as value.
    """
    task_results = {}
    for dn, ldap_entry in ldap_results.items():
        assert ldap_entry is not None, f"LDAP entry is None for dn: {dn}"
        parent = ldap_entry.get("supannCodeEntiteParent", [])
        if isinstance(parent, list):
            parent = parent[0] if parent else None
        if not isinstance(parent, str) or not parent:
            parent = None

        if parent:
            relationships = [{"type": "part_of", "subtype": None,
                              "target": f"local-{parent}",
                              "start_date": None, "end_date": None}]
        else:
            relationships = []

        task_results[dn] = {"relationships": relationships}
    return task_results
