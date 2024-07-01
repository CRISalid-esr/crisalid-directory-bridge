from airflow.decorators import task
from pendulum import DateTime


@task
def update_database_task(result, **kwargs):
    """Update the database with the converted LDAP entry.

    Args:
        converted_entry (dict): The converted LDAP entry.

    Returns:
        dict: The updated entry.
    """
    date: DateTime = kwargs.get('data_interval_start')
    timestamp = date.int_timestamp
    print(f"Updating database with result: {result} at {timestamp}")
    return result
