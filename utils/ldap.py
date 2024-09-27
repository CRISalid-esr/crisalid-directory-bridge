import logging

import ldap

from utils.config import get_env_variable

logger = logging.getLogger(__name__)


def connect_to_ldap() -> ldap:
    """
    Connect to the LDAP server using the environment variables for credentials.

    Returns: ldap connection
    """
    ldap_host = get_env_variable("LDAP_HOST")
    ldap_bind_dn = get_env_variable("LDAP_BIND_DN")
    ldap_bind_pw = get_env_variable("LDAP_BIND_PASSWORD")
    conn = ldap.initialize(ldap_host)
    conn.simple_bind_s(ldap_bind_dn, ldap_bind_pw)
    return conn


def ldap_response_to_json_dict(ldap_response, dict_key) -> dict:
    """Convert LDAP response to a JSON-serializable format.

    Args:
        ldap_response (list): The response from the LDAP server.

    Returns:
        list: A list of dictionaries with LDAP entries.
    """
    result = {}
    for dn, entry in ldap_response:
        logger.debug("Processing entry %s", dn)
        entry_key = entry.get(dict_key)
        if entry_key:
            entry_key = entry_key[0].decode('utf-8')
        else:
            logger.error("Missing uid for %s", dn)
            continue
        result[entry_key] = {
            k: [val.decode('utf-8') if isinstance(val, bytes) else val for val in v]
            for k, v in entry.items()}
    return result
