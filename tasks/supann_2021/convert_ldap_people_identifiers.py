import logging
import re

from airflow.decorators import task

logger = logging.getLogger(__name__)

"""
Regex validating eduPersonPrincipalName (ePPN) in the form local-part@domain.  
Local-part follows RFC 5322 dot-atom rules, and the domain follows RFC 1035 DNS syntax.
"""
EPPN_pattern = re.compile(r"^(?:[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+"
                          r"(?:\.[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+)*"
                          r')@'
                          r"(?:(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?\.)+"
                          r"[a-zA-Z]{2,})$"
                          )


@task(task_id="convert_ldap_people_identifiers")
def convert_ldap_people_identifiers(
        ldap_results: dict[str, dict[str, str | dict]],
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
        identifiers = entry.get('uid')
        if isinstance(identifiers, list) and len(identifiers) == 1:
            identifier = identifiers[0]
        else:
            logger.error("Invalid identifier for %s: %s", dn, identifiers)
            identifier = None

        eppn_values = entry.get('eduPersonPrincipalName')
        if not (isinstance(eppn_values, list) and len(eppn_values) == 1):
            eppn = ""
        else:
            eppn = eppn_values[0]
            if not isinstance(eppn, str):
                logger.error("Invalid eduPersonPrincipalName type for %s: %r", dn, eppn)
                eppn = ""
            elif not EPPN_pattern.fullmatch(eppn):
                logger.error("Invalid eduPersonPrincipalName format for %s: %s", dn, eppn)
                eppn = ""
        task_results[dn] = {"identifiers":
                [{"type": "local", "value": identifier},
                 *([{"type": "eduPersonPrincipalName", "value": eppn}] if eppn else []),
                 ]
        }
    return task_results
