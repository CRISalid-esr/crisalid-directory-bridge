import logging

from airflow.decorators import task

logger = logging.getLogger(__name__)


@task(task_id="convert_ldap_people_identifiers")
def convert_ldap_people_identifiers(ldap_results: dict[str, dict[str, str | dict]]) -> dict[
    str, dict[str, str | dict]
]:
    """
    Extract the 'identifier' field from a dict of ldap entries

    Args:
        ldap_results (dict): A dict of ldap results with dn as key and entry as value

    Returns:
        dict: A dict of converted results with the "identifiers" field populated
    """
    task_results = {}
    for dn, entry in ldap_results.items():
        identifiers = entry.get('uid')
        if isinstance(identifiers, list) and len(identifiers) == 1:
            identifier = identifiers[0]
        else:
            logger.error("Invalid identifier for %s: %s", dn, identifiers)
            identifier = None
        task_results[dn] = {"identifiers": [{
            "type": "local",
            "value": identifier
        }]}
    return task_results
