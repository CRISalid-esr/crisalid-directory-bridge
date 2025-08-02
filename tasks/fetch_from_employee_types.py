import logging
from airflow.decorators import task
from utils.yaml_loader import load_yaml

logger = logging.getLogger(__name__)


@task(task_id='fetch_from_employee_types')
def fetch_from_employee_types(employee_types_path: str) -> dict:
    """
    Load employee types configuration from a YAML file.

    This Airflow task reads a YAML file containing employee type definitions and
    returns its content as a dictionary.

    Args:
        employee_types_path (str): The file path to the YAML file containing
            employee type data.

    Returns:
        dict: A dictionary representation of the employee types YAML file.
    """
    return load_yaml(employee_types_path)


def _get_bodies_list(employee_types: dict) -> list[dict]:
    """
    Extract all 'corps' entries from the nested employee types dictionary.

    Args:
        employee_types (dict): A nested dictionary where keys represent statuses
            and categories, and values contain lists of corps definitions.

    Returns:
        list[dict]: A flat list of corps dictionaries aggregated from the employee types structure.
    """
    return [
        bodies
        for status in employee_types.values()
        for category in status.values()
        for bodies in category
    ]


@task(task_id='convert_employee_types_bodies')
def convert_employee_types_bodies(employee_types: dict) -> dict[str, tuple[str, str]]:
    """
    Convert employee types into a mapping of corps codes to labels.

    Args:
        employee_types (dict): A nested dictionary of employee type information.

    Returns:
        dict[str, tuple[str, str]]: A mapping of corps codes to tuples of
        (bodies_code, bodies_label).
    """
    bodies_list = _get_bodies_list(employee_types)

    return {
        bodies_entry["corps"]: (bodies_entry["corps"], bodies_entry["label"])
        for bodies_entry in bodies_list
        if bodies_entry.get("corps") is not None
    }


@task(task_id="convert_employee_types_local_values")
def convert_employee_types_local_value(employee_types: dict) -> dict[str, tuple[str, str]]:
    """
    Convert local values from employee types into a mapping of codes to corps and labels.

    Args:
        employee_types (dict): A nested dictionary of employee type information.

    Returns:
        dict[str, tuple[str, str]]: A mapping of local value codes to tuples of
        (bodies_code, bodies_label).
    """
    bodies_list = _get_bodies_list(employee_types)

    return {
        local_value: (bodies_entry["corps"], bodies_entry["label"])
        for bodies_entry in bodies_list
        if bodies_entry.get("local_values")  # only process if local_values exists and is not None
        for local_value in bodies_entry["local_values"]
    }
