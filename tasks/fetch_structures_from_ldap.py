import ldap
from airflow.decorators import task

from utils.config import get_env_variable
from utils.exceptions import LDAPConnectionError, LDAPSizeLimitExceededError
from utils.ldap import connect_to_ldap, ldap_response_to_json_dict


@task
def fetch_structures_from_ldap():
    """Fetch LDAP structures.

    Returns:
        list: A list of LDAP entries in JSON-serializable format.
    """
    ldap_connexion = connect_to_ldap()
    structures_branch = get_env_variable("LDAP_STRUCTURES_BRANCH")
    structures_filter = get_env_variable("LDAP_STRUCTURES_FILTER")
    try:
        ldap_response = ldap_connexion.search_s(structures_branch,
                                                ldap.SCOPE_SUBTREE,  # pylint: disable=no-member
                                                structures_filter)
    except ldap.SERVER_DOWN as error:  # pylint: disable=no-member
        raise LDAPConnectionError("Unable to connect to the LDAP server") from error
    except ldap.SIZELIMIT_EXCEEDED as error:  # pylint: disable=no-member
        raise LDAPSizeLimitExceededError("The LDAP response exceeds the size limit") from error
    return ldap_response_to_json_dict(ldap_response, dict_key='supannCodeEntite')
