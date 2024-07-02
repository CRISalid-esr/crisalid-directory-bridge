def import_task(task_name: str) -> callable:
    """
    Import an Airflow task from a module
    :param task_name: dot separated path to the task,
    e.g. tasks.supann_2021.convert_ldap_structure_name.convert_ldap_structure_name_task
    :return: The task function
    """
    module_name, function_name = task_name.rsplit('.', 1)
    print(f"Importing task {function_name} from module {module_name}")
    module = __import__(module_name, fromlist=[function_name])
    return getattr(module, function_name)
