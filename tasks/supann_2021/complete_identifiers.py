from airflow.decorators import task


@task(task_id="complete_identifiers")
def complete_identifiers(results, identifiers):
    """
    Complete the identifiers with the missing fields.

    Args:
        identifiers (dict): A dict of identifiers with dn as key and a dict with 'identifier'

    Returns:
        dict: A dict of identifiers with dn as key and a dict with 'identifier'
    """
