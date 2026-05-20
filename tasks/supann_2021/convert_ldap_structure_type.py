import os

from airflow.sdk import task

BUSINESS_CATEGORY_TO_MAIN_MISSION = {
    "administration": "administrative_services",
    "library": "scientific_services",
    "research": "research",
    "pedagogy": "teaching",
}


@task(task_id="convert_ldap_structure_type")
def convert_ldap_structure_type(ldap_results: dict) -> dict:
    """
    Return generic_type and (optionally) main_mission for each LDAP structure entry.

    generic_type is read from LDAP_STRUCTURE_GENERIC_TYPE (default: 'unit').
    main_mission is derived from the LDAP businessCategory field:
      administration → administrative_services
      library        → scientific_services
      research       → research
      pedagogy       → learning
      organization   → omitted (no main_mission)
    If businessCategory is absent or unmapped, main_mission is omitted.

    Args:
        ldap_results (dict): A dict of LDAP results with dn as key.

    Returns:
        dict: A dict with dn as key and {"generic_type": str[, "main_mission": str]} as value.
    """
    generic_type = os.getenv("LDAP_STRUCTURE_GENERIC_TYPE", "unit")
    task_results = {}
    for dn, ldap_entry in ldap_results.items():
        result = {"generic_type": generic_type}
        raw = ldap_entry.get("businessCategory", [])
        if isinstance(raw, list):
            raw = raw[0] if raw else None
        business_category = raw if isinstance(raw, str) and raw else None
        if business_category and business_category in BUSINESS_CATEGORY_TO_MAIN_MISSION:
            result["main_mission"] = BUSINESS_CATEGORY_TO_MAIN_MISSION[business_category]
        task_results[dn] = result
    return task_results
