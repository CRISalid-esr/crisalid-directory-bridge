# pylint: disable=duplicate-code
import pytest
from airflow.utils.state import TaskInstanceState

from test_utils.dags import create_dag_run, \
    create_task_instance, \
    DATA_INTERVAL_START, \
    DATA_INTERVAL_END

TEST_TASK_ID = "convert_ldap_structure_names"

TESTED_TASK_NAME = 'tasks.supann_2021.convert_ldap_structure_names.convert_ldap_structure_names'


@pytest.mark.parametrize("dag", [
    {
        "task_name": TESTED_TASK_NAME,
        "ldap_results": {
            "uid=1234,ou=people,dc=example,dc=org": {
                "eduorglegalname": ["University of Example"],
                "description": ["A university in Example"],
            },
        },
    }
], indirect=True)
def test_name_is_converted_from_ldap(dag, unique_execution_date):
    """
    Test that the name is converted from LDAP data
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
                    'language': 'fr',
                    'value': 'University of Example'
                }
            ]
        }
    }


# test that if eduorglegalname is not present, the description is used
@pytest.mark.parametrize("dag", [
    {
        "task_name": TESTED_TASK_NAME,
        "ldap_results": {
            "uid=1234,ou=people,dc=example,dc=org": {
                "description": ["A university in Example"],
            },
        },
    }
], indirect=True)
def test_description_used_if_name_not_present(dag, unique_execution_date):
    """
    Test that if the eduorglegalname field is not present, the description field is used instead.
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
                    'language': 'fr',
                    'value': 'A university in Example'
                }
            ]
        }}


@pytest.mark.parametrize("dag", [
    {
        "task_name": TESTED_TASK_NAME,
        "ldap_results": {
            "uid=1234,ou=people,dc=example,dc=org": {
                "eduorglegalname": ["University of Example"],
                "description": ["A university in Example"],
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
                    'language': 'en',
                    'value': 'University of Example'
                }
            ]
        }
    }
