from airflow.decorators import task

COMPLETING_IDENTIFIERS = ['id_hal_i', 'id_hal_s', 'orcid', 'idref', 'scopus_eid']
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

    completed_identifiers = {}
    seen_identifiers = []

    for key, value in results.items():
        value_to_save = value
        for identifier in value['identifiers']:
            if identifier['type'] == 'local':
                if identifier not in seen_identifiers:
                    seen_identifiers.append(identifier)

                    local = identifier['value']
                    for ident in identifiers_list:
                        if local == ident["local"]:
                            non_empty_identifiers = [
                                {'type': identifier, 'value': ident[identifier]}
                                for identifier in COMPLETING_IDENTIFIERS if ident.get(identifier) and ident[identifier].strip()
                            ]
                            value_to_save["identifiers"].extend(non_empty_identifiers)

                    completed_identifiers[key] = value_to_save


    return completed_identifiers
