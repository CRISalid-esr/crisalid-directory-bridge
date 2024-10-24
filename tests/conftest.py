import os
from os import environ

import pendulum
import pytest
from _pytest.fixtures import SubRequest
from airflow import settings, DAG
from airflow.models import DagBag
from airflow.utils.db import resetdb

from test_utils.dags import TEST_DAG_ID, DATA_INTERVAL_START
from utils.dependencies import import_from_path

environ["APP_ENV"] = "TEST"


@pytest.fixture(name="unique_execution_date")
def unique_execution_date() -> pendulum.DateTime:
    """
    Get a unique execution date to avoid conflicts between tests
    :return: The unique execution date
    """
    return pendulum.now()


@pytest.fixture(scope='function', autouse=True, name="initialize_airflow_db")
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
    airflow_home = os.getenv('AIRFLOW_HOME')
    assert airflow_home is not None, "AIRFLOW_HOME is not set"
    dag_folder = f"{airflow_home}/dags"
    print(f"Dag folder is : {dag_folder}")
    return DagBag(dag_folder=dag_folder)


@pytest.fixture(name="dag")
def dag_fixture(request: SubRequest) -> DAG:
    """
    Create a DAG for testing the convert_ldap_structure_description_task
    :param request: The pytest request object
    It is a dictionary with the following keys
    - task_name: The name of the task to be created
    - param_names: The names of the parameters to be passed to the task
    - other keys: The values of the parameters to be passed to the task
    :return: The DAG object
    """
    task_name = request.param['task_name']
    param_names = request.param['param_names']
    assert isinstance(param_names, list)
    assert all(isinstance(name, str) for name in param_names)
    args = [request.param[name] for name in param_names]
    with DAG(
            dag_id=TEST_DAG_ID,
            schedule="@daily",
            start_date=DATA_INTERVAL_START,
    ) as created_dag:
        task = import_from_path(task_name)
        task(*args)
    return created_dag

# @pytest.fixture(name="double_arg_dag")
# def double_argument_dag_fixture(request: SubRequest) -> DAG:
#     """
#     Create a DAG for testing the convert_ldap_structure_description_task
#     :param request: The pytest request object
#     :return: The DAG object
#     """
#     task_name = request.param['task_name']
#     raw_results = request.param['raw_results']
#     identifiers_list = request.param['identifiers_list']
#     with DAG(
#             dag_id=TEST_DAG_ID,
#             schedule="@daily",
#             start_date=DATA_INTERVAL_START,
#     ) as created_dag:
#         convert_description_task = import_from_path(task_name)
#         convert_description_task(raw_results, identifiers_list)
#     return created_dag
