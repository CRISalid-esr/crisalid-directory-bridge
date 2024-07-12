import logging

from airflow.decorators import task

logger = logging.getLogger(__name__)


@task(task_id="convert_ldap_structure_identifier")
def convert_ldap_structure_identifier_task(ldap_result: dict[str, str | dict]) -> str:
    """Extract the 'acroynm' field from an LDAP entry.

    Args:
        ldap_result (dict): An LDAP result with "dn" and "entry" fields.

    Returns:
        str: The 'acronym' field.
    """
    ldap_entry = ldap_result.get('entry', None)
    assert ldap_entry is not None, "LDAP entry is None"
    assert 'supannCodeEntite' in ldap_entry, f"missing supannCodeEntite in {ldap_entry}"
    return ldap_entry.get('supannCodeEntite', '')
