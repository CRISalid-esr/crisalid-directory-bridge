# pylint: disable=duplicate-code
import pytest
from airflow.utils.state import TaskInstanceState

from test_utils.dags import create_dag_run, \
    create_task_instance, \
    DATA_INTERVAL_START, \
    DATA_INTERVAL_END

TEST_TASK_ID = "convert_ldap_structure_long_labels"

TESTED_TASK_NAME = ('tasks.supann_2021.convert_ldap_structure_long_labels'
                    '.convert_ldap_structure_long_labels')


@pytest.mark.parametrize("dag", [
    {
        "task_name": TESTED_TASK_NAME,
        "param_names": ["raw_results"],
        "raw_results": {
            "uid=1234,ou=people,dc=example,dc=org": {
                "eduorglegalname": ["University of Example"],
                "description": ["A university in Example"],
            },
        },
    }
], indirect=True)
def test_name_is_converted_from_ldap(dag, unique_logical_date):
    """Test that the name is converted from LDAP data."""
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_logical_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids=TEST_TASK_ID) == {
        "uid=1234,ou=people,dc=example,dc=org": {
            "long_labels": [{'language': 'fr', 'value': 'University of Example'}]
        }
    }


@pytest.mark.parametrize("dag", [
    {
        "task_name": TESTED_TASK_NAME,
        "param_names": ["raw_results"],
        "raw_results": {
            "uid=1234,ou=people,dc=example,dc=org": {
                "description": ["A university in Example"],
            },
        },
    }
], indirect=True)
def test_description_used_if_name_not_present(dag, unique_logical_date):
    """Test that if eduorglegalname is not present, the description field is used instead."""
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_logical_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids=TEST_TASK_ID) == {
        "uid=1234,ou=people,dc=example,dc=org": {
            "long_labels": [{'language': 'fr', 'value': 'A university in Example'}]
        }
    }


@pytest.mark.parametrize("dag", [
    {
        "task_name": TESTED_TASK_NAME,
        "param_names": ["raw_results"],
        "raw_results": {
            "uid=1234,ou=people,dc=example,dc=org": {
                "eduorglegalname": ["University of Example"],
                "description": ["A university in Example"],
            },
        },
    }
], indirect=True)
def test_language_is_set_to_en(dag, unique_logical_date, monkeypatch):
    """Test that LDAP_DEFAULT_LANGUAGE env variable controls the language field."""
    monkeypatch.setenv("LDAP_DEFAULT_LANGUAGE", "en")
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_logical_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids=TEST_TASK_ID) == {
        "uid=1234,ou=people,dc=example,dc=org": {
            "long_labels": [{'language': 'en', 'value': 'University of Example'}]
        }
    }
