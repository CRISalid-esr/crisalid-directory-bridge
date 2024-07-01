import os
from os import environ

import pytest as pytest
from airflow import settings, DAG
from airflow.models import DagBag
from airflow.utils.db import resetdb

environ["APP_ENV"] = "TEST"


@pytest.fixture(scope='function', autouse=True)
def initialize_airflow_db() -> None:
    """
    Initialize the airflow database for testing and reset the database before each test
    :return: None
    """
    os.environ['AIRFLOW__CORE__SQL_ALCHEMY_CONN'] = "sqlite:////:memory:"
    resetdb()
    settings.configure_orm()


@pytest.fixture(name="dagbag")
def fixture_dagbag() -> DagBag:
    """
    Create a DagBag for testing dag loading
    :return: DagBag
    """
    return DagBag()


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
