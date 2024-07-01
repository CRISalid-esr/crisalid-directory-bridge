from airflow.decorators import task


@task
def combine_results(names: list[str], acronyms: list[str], descriptions: list[str], addresses: list[str]) -> list[
    dict[str, str]]:
    """
    Combine the results of the LDAP conversion tasks into a single JSON structure.
    :param names:
    :param acronyms:
    :param descriptions:
    :param addresses:
    :return:
    """
    combined = []
    for name, acronym, description, address in zip(names, acronyms, descriptions, addresses):
        combined.append({
            "name": name,
            "acronym": acronym,
            "description": description,
            "address": address
        })
    return combined
