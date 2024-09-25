import logging

from airflow.decorators import task

from utils.list import deduplicate_dict_list

COMPLETING_IDENTIFIERS = ['id_hal_i', 'id_hal_s', 'orcid', 'idref', 'scopus_eid']

logger = logging.getLogger(__name__)


@task(task_id="complete_identifiers")
def complete_identifiers(ldap_source: dict, identifiers_spreadsheet: dict) -> dict:
    """
    Complete the identifiers with the missing fields.

    Args:
        ldap_source (dict): the converted content from ldap source
        identifiers_spreadsheet (dict): A list of identifiers

    Returns:
        dict: A dict of identifiers with dn as key and a dict with 'identifier'
    """

    identifiers_dict = {f"uid={ident['local']}": [{"type": k, "value": v} for k, v in ident.items()]
                        for ident in identifiers_spreadsheet}

    for key, value in ldap_source.items():
        ldap_source[key]['identifiers'] = deduplicate_dict_list(
            value['identifiers'] + identifiers_dict[key])
        ldap_source[key]['identifiers'].sort(key=lambda x: x['type'])
    logger.info("Identifiers completed !")

    return ldap_source
