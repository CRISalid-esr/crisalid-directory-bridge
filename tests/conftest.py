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
    airflow_home = os.getenv('AIRFLOW_HOME')
    assert airflow_home is not None, "AIRFLOW_HOME is not set"
    dag_folder = f"{airflow_home}/dags"
    print(f"Dag folder is : {dag_folder}")
    return DagBag(dag_folder=dag_folder)
