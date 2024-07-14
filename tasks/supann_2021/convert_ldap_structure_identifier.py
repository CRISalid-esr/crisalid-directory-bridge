import logging

from airflow.decorators import task

logger = logging.getLogger(__name__)


@task(task_id="convert_ldap_structure_identifier")
def convert_ldap_structure_identifier_task(ldap_result: dict[str, str | dict]) -> list[dict]:
    """Extract the 'acroynm' field from an LDAP entry.

    Args:
        ldap_result (dict): An LDAP result with "dn" and "entry" fields.

    Returns:
        str: The 'acronym' field.
    """
    identifiers = []
    ldap_entry = ldap_result.get('entry', None)
    assert ldap_entry is not None, "LDAP entry is None"
    assert 'supannCodeEntite' in ldap_entry, f"missing supannCodeEntite in {ldap_entry}"
    identifiers.append({"type": "local", "value": ldap_entry["supannCodeEntite"]})
    if 'supannRefId' in ldap_entry:
        ref_id = ldap_entry['supannRefId']
        if ref_id.startswith('{RNSR}'):
            identifiers.append({"type": "RNSR", "value": ref_id[6:]})
    return identifiers
