import logging

from airflow.sdk import task

from utils.list import deduplicate_dict_list

COMPLETING_IDENTIFIERS = ['idhali', 'idhals', 'orcid', 'idref', 'scopus']

logger = logging.getLogger(__name__)


@task(task_id="complete_identifiers")
def complete_identifiers(ldap_source: dict, identifiers_spreadsheet: list[dict[str, str]]) -> dict:
    """
    Complete the identifiers with the missing fields.

    Args:
        ldap_source (dict): the converted content from ldap source
        identifiers_spreadsheet (dict): A list of identifiers

    Returns:
        dict: A dict of identifiers with dn as key and a dict with 'identifier'
    """

    identifiers_dict = {f"{ident['local']}": [{"type": k, "value": v} for k, v in ident.items()]
                        for ident in identifiers_spreadsheet}

    for key, value in ldap_source.items():
        if key not in identifiers_dict:
            logger.error("Local identifier %s not found in complementary identifiers", key)
            continue
        logger.info("Completing identifiers for %s", key)
        ldap_source[key]['identifiers'] = deduplicate_dict_list(
            value['identifiers'] + identifiers_dict[key])
        ldap_source[key]['identifiers'] = [ident for ident in ldap_source[key]['identifiers'] if
                                           ident['value']]
        ldap_source[key]['identifiers'].sort(key=lambda x: x['type'])
    logger.info("Identifiers completed !")

    return ldap_source
