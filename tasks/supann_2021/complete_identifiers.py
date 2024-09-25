import logging

from airflow.decorators import task

from utils.list import deduplicate_dict_list

COMPLETING_IDENTIFIERS = ['id_hal_i', 'id_hal_s', 'orcid', 'idref', 'scopus_eid']

logger = logging.getLogger(__name__)


@task(task_id="complete_identifiers")
def complete_identifiers(results, identifiers_list):
    """
    Complete the identifiers with the missing fields.

    Args:
        results: temp
        identifiers_list (dict): A dict of identifiers with dn as key and a dict with 'identifier'

    Returns:
        dict: A dict of identifiers with dn as key and a dict with 'identifier'
    """

    identifiers_dict = {f"uid={ident['local']}": [{"type": k, "value": v} for k, v in ident.items()]
                        for ident in identifiers_list}

    for key, value in results.items():
        results[key]['identifiers'] = deduplicate_dict_list(
            value['identifiers'] + identifiers_dict[key])

    logger.info("Identifiers completed !")

    return results
