import logging

from airflow.decorators import task

from utils.config import get_env_variable

logger = logging.getLogger(__name__)


@task(task_id="convert_ldap_structure_name_task")
def convert_ldap_structure_name_task(ldap_result: dict[str, str | dict]) -> list[dict]:
    """Extract the 'acroynm' field from an LDAP entry.

    Args:
        ldap_result (dict): An LDAP result with "dn" and "entry" fields.

    Returns:
        str: The 'acronym' field.
    """
    ldap_entry = ldap_result.get('entry', None)
    assert ldap_entry is not None, "LDAP entry is None"
    name = ldap_entry.get('eduorglegalname', ldap_entry.get('description', ''))
    language = get_env_variable("LDAP_DEFAULT_LANGUAGE")
    return [{"value": name, "language": language}]
