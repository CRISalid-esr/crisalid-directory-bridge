import logging

from airflow.decorators import task
from ldap3 import SUBTREE
from ldap3.core.exceptions import LDAPExceptionError

from utils.config import get_env_variable
from utils.exceptions import LDAPError
from utils.ldap import connect_to_ldap, ldap_response_to_json_dict

logger = logging.getLogger(__name__)


@task
def fetch_ldap_people():
    """Fetch LDAP people.

    Returns:
        dict: A dictionary of LDAP entries in JSON-serializable format.
    """
    ldap_connexion = connect_to_ldap()
    people_branch = get_env_variable("LDAP_PEOPLE_BRANCH")
    people_filters_str = get_env_variable("LDAP_PEOPLE_FILTERS")
    people_filter_pattern = get_env_variable("LDAP_PEOPLE_FILTER_PATTERN")
    people_filters = people_filters_str.split(',')
    ldap_response = []
    attributes = ["uid", "cn", "displayName", "sn", "givenName", "mail",
                  "supannRefid", "description", "eduPersonORCID",
                  "supannEntiteAffectationPrincipale", "supannEntiteAffectation",
                  "eduPersonOrgUnitDN", "eduPersonPrimaryOrgUnitDN",
                  "employeeType","supannEmpCorps", "eduPersonAffiliation",
                  "eduPersonPrimaryAffiliation","supannEmpProfil",
                  "supannEtablissement", "eduPersonOrgDN",
                  "postalAddress", "labeledURI", "eduOrgHomePageURI"]
    for people_filter in people_filters:
        people_filter = people_filter_pattern % people_filter
        try:
            # Perform LDAP search
            ldap_connexion.search(
                search_base=people_branch,
                search_filter=people_filter,
                search_scope=SUBTREE,
                attributes=attributes
            )
            ldap_partial_response = ldap_connexion.entries
            ldap_response.extend(ldap_partial_response)
        except LDAPExceptionError as error:
            raise LDAPError(
                "Unable to connect to the LDAP server "
                f"while fetching people with filter {people_filter}"
            ) from error

    # Convert the response to a JSON-serializable format
    formatted_result = ldap_response_to_json_dict(ldap_response, dict_key='uid')
    return formatted_result
