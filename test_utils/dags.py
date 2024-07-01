from airflow import DAG


def assert_dag_dict_equal(structure: dict, dag: DAG) -> None:
    """
    Controls the internal structure of the DAG by comparing the task_dict
    to a provided dictionary
    :param structure: Dictionary containing the structure of the DAG
    :param dag: DAG object to compare
    :return: None
    """
    assert dag.task_dict.keys() == structure.keys()
    for task_id, downstream_list in structure.items():
        assert dag.has_task(task_id)
        task = dag.get_task(task_id)
        assert task.downstream_task_ids == set(downstream_list)
