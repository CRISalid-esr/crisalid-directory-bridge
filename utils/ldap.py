import logging

from ldap3 import Server, Connection, AUTO_BIND_NO_TLS, ALL, get_config_parameter, \
    set_config_parameter

from utils.config import get_env_variable

logger = logging.getLogger(__name__)


def connect_to_ldap() -> Connection:
    """
    Connect to the LDAP server using the environment variables for credentials.

    Returns: ldap3 connection
    """
    ldap_host = get_env_variable("LDAP_HOST")
    ldap_bind_dn = get_env_variable("LDAP_BIND_DN")
    ldap_bind_pw = get_env_variable("LDAP_BIND_PASSWORD")
    server = Server(ldap_host, get_info=ALL)
    conn = Connection(server, ldap_bind_dn, ldap_bind_pw, auto_bind=AUTO_BIND_NO_TLS)
    # acronym attribute for structures raises an error for unknown reason
    attrs = get_config_parameter('ATTRIBUTES_EXCLUDED_FROM_CHECK')
    attrs.extend(['acronym'])
    set_config_parameter('ATTRIBUTES_EXCLUDED_FROM_CHECK', attrs)
    return conn


def ldap_response_to_json_dict(ldap_response, dict_key) -> dict:
    """Convert LDAP response to a JSON-serializable format.

    Args:
        ldap_response (list): The response from the LDAP server.

    Returns:
        dict: A dictionary with LDAP entries.
    """
    result = {}
    for entry in ldap_response:
        dn = entry.entry_dn
        logger.debug("Processing entry %s", dn)
        entry_key = entry[dict_key]
        if entry_key:
            entry_key = entry_key[0]
        else:
            logger.error(f"Missing <{dict_key}> for %s", dn)
            continue
        result[entry_key] = dict(entry.entry_attributes_as_dict.items())
    return result
