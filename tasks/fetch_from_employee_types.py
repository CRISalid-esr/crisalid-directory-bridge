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
    Extract all 'bodies' entries from the nested employee types dictionary.

    Args:
        employee_types (dict): A nested dictionary where keys represent statuses
            and categories, and values contain lists of body definitions.

    Returns:
        list[dict]: A flat list of body dictionaries aggregated from the employee types structure.
    """
    bodies_list = [
        body
        for status in employee_types.values()
        for category in status.values()
        for body in category
    ]
    return bodies_list


@task(task_id='convert_employee_types_bodies')
def convert_employee_types_bodies(employee_types: dict) -> dict[str, tuple[str, str]]:
    """
    Convert employee types into a mapping of corps codes to labels.

    This Airflow task extracts 'corps' data from the employee types and creates a
    dictionary mapping each corps code to a tuple containing (corps_code, label).

    Args:
        employee_types (dict): A nested dictionary of employee type information.

    Returns:
        dict[str, tuple[str, str]]: A mapping of corps codes to tuples of
        (corps_code, corps_label).
    """
    bodies_list = _get_bodies_list(employee_types)
    bodies_position = {}
    for bodies in bodies_list:
        if bodies["corps"] is not None:
            bodies_position[bodies["corps"]] = (bodies["corps"], bodies["label"])
    return bodies_position


@task(task_id="convert_employee_types_local_values")
def convert_employee_types_local_value(employee_types: dict) -> dict[str, tuple[str, str]]:
    """
    Convert local values from employee types into a mapping of codes to corps and labels.

    This Airflow task processes the 'local_values' field from the employee types and
    creates a dictionary mapping each local value to its corresponding corps code and label.

    Args:
        employee_types (dict): A nested dictionary of employee type information.

    Returns:
        dict[str, tuple[str, str]]: A mapping of local value codes to tuples of
        (corps_code, corps_label).
    """
    bodies_list = _get_bodies_list(employee_types)
    local_value_position = {}
    for bodies in bodies_list:
        if "local_values" in bodies and bodies["local_values"] is not None:
            for local_value in bodies["local_values"]:
                local_value_position[local_value] = (bodies["corps"], bodies["label"])
    return local_value_position
