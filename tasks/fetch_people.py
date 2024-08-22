import logging

import ldap
from airflow.decorators import task

from utils.config import get_env_variable
from utils.exceptions import LDAPConnectionError, LDAPSizeLimitExceededError
from utils.ldap import connect_to_ldap, ldap_response_to_json_dict

logger = logging.getLogger(__name__)


@task
def fetch_people():
    """Fetch LDAP people.

    Returns:
        list: A list of LDAP entries in JSON-serializable format.
    """
    ldap_connexion = connect_to_ldap()
    people_branch = get_env_variable("LDAP_PEOPLE_BRANCH")
    people_filters_str = get_env_variable("LDAP_PEOPLE_FILTERS")
    people_filter_pattern = get_env_variable(
        "LDAP_PEOPLE_FILTER_PATTERN")
    people_filters = people_filters_str.split(',')
    ldap_response = []
    attributes = ["uid", "cn", "displayName", "sn", "givenName", "mail",
                  "supannRefid", "description", "eduPersonORCID",
                  "supannEntiteAffectationPrincipale", "supannEntiteAffectation",
                  "eduPersonOrgUnitDN", "eduPersonPrimaryOrgUnitDN",
                  "employeeType", "eduPersonAffiliation", "eduPersonPrimaryAffiliation",
                  "supannEtablissement", "eduPersonOrgDN",
                  "postalAddress", "labeledURI", "eduOrgHomePageURI"]
    for people_filter in people_filters:
        people_filter = people_filter_pattern % people_filter
        try:
            ldap_partial_response = ldap_connexion.search_s(people_branch,
                                                            ldap.SCOPE_SUBTREE,  # pylint: disable=no-member
                                                            people_filter,
                                                            attrlist=attributes
                                                            )
            ldap_response.extend(ldap_partial_response)
        except ldap.SERVER_DOWN as error:  # pylint: disable=no-member
            raise LDAPConnectionError(
                "Unable to connect to the LDAP server "
                f"while fetching people with filter {people_filter}") from error
        except ldap.SIZELIMIT_EXCEEDED as error:  # pylint: disable=no-member
            raise LDAPSizeLimitExceededError(
                "The LDAP response exceeds the size limit"
                f"for people filter {people_filter}") from error
    formatted_result = ldap_response_to_json_dict(ldap_response)
    return formatted_result
