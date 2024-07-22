# pylint: disable=duplicate-code
import pytest
from airflow.utils.state import TaskInstanceState

from test_utils.dags import create_dag_run, \
    create_task_instance, \
    DATA_INTERVAL_START, \
    DATA_INTERVAL_END

TEST_TASK_ID = "convert_ldap_people_names"

TESTED_TASK_NAME = 'tasks.supann_2021.convert_ldap_people_names.convert_ldap_people_names'


@pytest.mark.parametrize("dag", [
    {
        "task_name": TESTED_TASK_NAME,
        "ldap_results": {
            "uid=1234,ou=people,dc=example,dc=org": {
                "sn": "Doe",
                "givenName": "John",
            },
        },
    }
], indirect=True)
def test_names_are_converted_from_ldap(dag, unique_execution_date):
    """
    Test that the first and last names are converted from LDAP data
    :param dag:
    :param unique_execution_date:
    :return:
    """
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_execution_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids=TEST_TASK_ID) == {
        "uid=1234,ou=people,dc=example,dc=org": {
            "names": [
                {
                    "last_names": [{"value": "Doe", "language": "fr"}],
                    "first_names": [{"value": "John", "language": "fr"}]
                }
            ]
        }
    }


@pytest.mark.parametrize("dag", [
    {
        "task_name": TESTED_TASK_NAME,
        "ldap_results": {
            "uid=1234,ou=people,dc=example,dc=org": {
                "sn": "Doe",
            },
        },
    }
], indirect=True)
def test_only_last_name_present(dag, unique_execution_date):
    """
    Test that if only the last name is present, it is converted correctly.
    :param dag: The DAG object
    :param unique_execution_date: unique execution date
    :return: None
    """
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_execution_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids=TEST_TASK_ID) == {
        "uid=1234,ou=people,dc=example,dc=org": {
            "names": [
                {
                    "last_names": [{"value": "Doe", "language": "fr"}],
                    "first_names": [{"value": None, "language": "fr"}]
                }
            ]
        }
    }


@pytest.mark.parametrize("dag", [
    {
        "task_name": TESTED_TASK_NAME,
        "ldap_results": {
            "uid=1234,ou=people,dc=example,dc=org": {
                "givenName": "John",
            },
        },
    }
], indirect=True)
def test_only_first_name_present(dag, unique_execution_date):
    """
    Test that if only the first name is present, it is converted correctly.
    :param dag: The DAG object
    :param unique_execution_date: unique execution date
    :return: None
    """
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_execution_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids=TEST_TASK_ID) == {
        "uid=1234,ou=people,dc=example,dc=org": {
            "names": [
                {
                    "last_names": [{"value": None, "language": "fr"}],
                    "first_names": [{"value": "John", "language": "fr"}]
                }
            ]
        }
    }


@pytest.mark.parametrize("dag", [
    {
        "task_name": TESTED_TASK_NAME,
        "ldap_results": {
            "uid=1234,ou=people,dc=example,dc=org": {
                "sn": "Doe",
                "givenName": "John",
            },
        },
    }
], indirect=True)
def test_language_is_set_to_en(dag, unique_execution_date, monkeypatch):
    """
    Test that if the LDAP_DEFAULT_LANGUAGE environment variable is set to 'en',
    the language is set to 'en'.
    :param dag: The DAG object
    :param unique_execution_date:
    :param monkeypatch: The monkeypatch fixture
    :return: None
    """
    monkeypatch.setenv("LDAP_DEFAULT_LANGUAGE", "en")
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_execution_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids=TEST_TASK_ID) == {
        "uid=1234,ou=people,dc=example,dc=org": {
            "names": [
                {
                    "last_names": [{"value": "Doe", "language": "en"}],
                    "first_names": [{"value": "John", "language": "en"}]
                }
            ]
        }
    }
