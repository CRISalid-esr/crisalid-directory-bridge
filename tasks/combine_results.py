from airflow.decorators import task


@task
def combine_results(task_results: dict[str, list[str]]) -> list[dict[str, str]]:
    """
    Combine the results of the LDAP conversion tasks into a single JSON structure.
    :param task_results: A dictionary containing lists of results from various tasks.
    :return: A list of combined results as dictionaries.
    """
    combined = []
    keys = task_results.keys()
    for values in zip(*task_results.values()):
        combined.append({key.lower(): value for key, value in zip(keys, values)})
    return combined
