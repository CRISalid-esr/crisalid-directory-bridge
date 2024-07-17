import logging

from airflow.decorators import task

logger = logging.getLogger(__name__)


@task(task_id="convert_ldap_structure_identifiers")
def convert_ldap_structure_identifiers(ldap_results: dict[str, dict[str, str | dict]]) -> dict[
    str, dict[str, list[dict[str, str]]]]:
    """
    Extract identifiers from a dict of LDAP entries for structures.

    Args:
        ldap_results (dict): A dict of LDAP results with dn as key and entry as value.

    Returns:
        dict: A dict of converted results with dn as key
        and a dict with identifiers field populated as value.
    """
    task_results = {}
    for dn, ldap_entry in ldap_results.items():
        assert 'supannCodeEntite' in ldap_entry, \
            f"missing supannCodeEntite in {ldap_entry} for dn: {dn}"
        identifiers = [{"type": "local", "value": ldap_entry["supannCodeEntite"]}]
        if 'supannRefId' in ldap_entry:
            ref_id = ldap_entry['supannRefId']
            if ref_id.startswith('{RNSR}'):
                identifiers.append({"type": "RNSR", "value": ref_id[6:]})
        task_results[dn] = {"identifiers": identifiers}

    return task_results
