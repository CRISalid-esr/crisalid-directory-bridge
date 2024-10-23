import logging

from airflow.decorators import task
from ldap3 import SUBTREE
from ldap3.core.exceptions import LDAPExceptionError

from utils.config import get_env_variable
from utils.exceptions import LDAPError
from utils.ldap import connect_to_ldap, ldap_response_to_json_dict

logger = logging.getLogger(__name__)


@task
def fetch_structures_from_ldap():
    """Fetch LDAP structures.

    Returns:
        dict: A dictionary of LDAP entries in JSON-serializable format.
    """
    ldap_connexion = connect_to_ldap()
    structures_branch = get_env_variable("LDAP_STRUCTURES_BRANCH")
    structures_filter = get_env_variable("LDAP_STRUCTURES_FILTER")

    try:
        ldap_connexion.search(
            search_base=structures_branch,
            search_filter=structures_filter,
            search_scope=SUBTREE,
            attributes=["supannCodeEntite",
                        "eduOrgLegalName",
                        "ou",
                        "description",
                        "acronym",
                        "postalAddress",
                        "labeledURI",
                        "supannRefId"]
        )
        ldap_response = ldap_connexion.entries

    except LDAPExceptionError as error:
        raise LDAPError("Unable to connect to the LDAP server") from error

    # Convert the response to a JSON-serializable format
    logger.info("Fetched LDAP structures : %s", ldap_response)
    return ldap_response_to_json_dict(ldap_response, dict_key='supannCodeEntite')
