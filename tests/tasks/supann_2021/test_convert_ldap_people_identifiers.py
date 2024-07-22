# pylint: disable=duplicate-code
import pytest
from airflow.utils.state import TaskInstanceState

from test_utils.dags import create_dag_run, \
    create_task_instance, \
    DATA_INTERVAL_START, \
    DATA_INTERVAL_END

TEST_TASK_ID = "convert_ldap_people_identifiers"

TESTED_TASK_NAME = 'tasks.supann_2021.convert_ldap_people_identifiers' \
                   '.convert_ldap_people_identifiers'


@pytest.mark.parametrize("dag", [
    {
        "task_name": TESTED_TASK_NAME,
        "ldap_results": {
            "uid=1234,ou=people,dc=example,dc=org": {
                "uid": "1234",
            },
        },
    }
], indirect=True)
def test_identifier_is_converted_from_ldap(dag, unique_execution_date):
    """
    Test that the identifier is converted from LDAP data
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
            "identifiers": [
                {
                    "type": "local",
                    "value": "1234"
                }
            ]
        }
    }


@pytest.mark.parametrize("dag", [
    {
        "task_name": TESTED_TASK_NAME,
        "ldap_results": {
            "uid=1234,ou=people,dc=example,dc=org": {},
        },
    }
], indirect=True)
def test_identifier_absent(dag, unique_execution_date):
    """
    Test that if the identifier is not present, the value is None.
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
            "identifiers": [
                {
                    "type": "local",
                    "value": None
                }
            ]
        }
    }
