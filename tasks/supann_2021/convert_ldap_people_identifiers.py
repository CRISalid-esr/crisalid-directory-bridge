import logging
import re

from airflow.sdk import task

logger = logging.getLogger(__name__)

r"""
Regex validating eduPersonPrincipalName (ePPN) in the form local-part@domain.

^[a-zA-Z0-9._-]+   → Matches the beginning of the email (username part),
                       allowing letters, numbers, dots, underscores,
                       and hyphens.
@                     → Ensures the presence of the @ symbol.
[a-zA-Z0-9.-]+        → Matches the domain name, which can include letters,
                       numbers, dots, and hyphens.
\.[a-zA-Z]{2,}$       → Ensures a valid top-level domain (TLD)
                       with at least two letters, such as .fr, .com or .org.
"""

EPPN_PATTERN = re.compile(r'^[a-zA-Z0-9._-]+'
                          r'@'
                          r'[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')


def extract_valid_eppn(entry: dict[str, list[str]]) -> str | None:
    """
    Extract and validate the eduPersonPrincipalName from an LDAP entry.
    """
    eppn_values = entry.get('eduPersonPrincipalName')
    if not (isinstance(eppn_values, list) and len(eppn_values) == 1):
        return None
    eppn = eppn_values[0]
    if not isinstance(eppn, str):
        logger.warning("Invalid eduPersonPrincipalName type: %r", eppn)
        return None
    if not EPPN_PATTERN.fullmatch(eppn):
        logger.warning("Invalid eduPersonPrincipalName format: %s", eppn)
        return None
    return eppn


@task(task_id="convert_ldap_people_identifiers")
def convert_ldap_people_identifiers(
        ldap_results: dict[str, dict[str, list[str]]],
        config: dict[str, tuple[str, str]] = None
) -> dict[
    str, dict[str, list[dict[str, str | None]]]
]:
    """
    Extract the 'identifier' field from a dict of ldap entries

    Args:
        ldap_results (dict): A dict of ldap results with dn as key and entry as value
        config (dict): A configuration dict,
            not used in this function but required by the task signature

    Returns:
        dict: A dict of converted results with the "identifiers" field populated
    """
    _ = config
    task_results = {}
    for dn, entry in ldap_results.items():
        identifiers_list=[]
        identifiers = entry.get('uid')
        if isinstance(identifiers, list) and len(identifiers) == 1:
            identifier = identifiers[0]
        else:
            logger.error("Invalid identifier for %s: %s", dn, identifiers)
            identifier = None
        identifiers_list.append({"type": "local", "value": identifier})

        eppn = extract_valid_eppn(entry)
        if eppn is not None:
            identifiers_list.append({'type': 'eppn', 'value': eppn})
        task_results[dn] = {"identifiers": identifiers_list}

    return task_results
