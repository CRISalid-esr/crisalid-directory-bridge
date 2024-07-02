import ldap

from utils.config import get_env_variable


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


def ldap_response_to_json(ldap_response):
    """Convert LDAP response to a JSON-serializable format.

    Args:
        ldap_response (list): The response from the LDAP server.

    Returns:
        list: A list of dictionaries with LDAP entries.
    """
    result = []
    for dn, entry in ldap_response:
        entry_dict = {k: v[0].decode('utf-8') if isinstance(v[0], bytes) else v[0]
                      for k, v in entry.items()}
        result.append({'dn': dn, 'entry': entry_dict})
    return result
