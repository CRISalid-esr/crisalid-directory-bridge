import os
from os import environ

import pytest
from airflow import settings
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
    dag_folder = f"{os.getenv('AIRFLOW_HOME')}/dags"
    print(f"*********Dag folder: {dag_folder}*********")
    # print current directory
    print(f"*********Current file folder: {os.getcwd()}*********")
    # print directory of this file
    print(f"*********Current file folder: {os.path.dirname(os.path.abspath(__file__))}*********")
    return DagBag(dag_folder=dag_folder)
