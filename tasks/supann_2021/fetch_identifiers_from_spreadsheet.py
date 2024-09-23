from airflow.decorators import task


@task(task_id="fetch_identifiers_from_spreadsheet")
def fetch_identifiers_from_spreadsheet() -> dict[str, dict[str, str]]:
    """
    Fetch the identifiers from a spreadsheet.

    Returns:
        dict: A dict of identifiers with dn as key and a dict with 'identifier'
    """
