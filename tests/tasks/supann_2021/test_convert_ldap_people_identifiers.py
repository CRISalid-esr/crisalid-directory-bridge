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
        "param_names": ["raw_results"],
        "raw_results": {
            "uid=1234,ou=people,dc=example,dc=org": {
                "uid": ["1234"],
                "eduPersonPrincipalName": ["1234@example.org"]
            },
        },
    }
], indirect=True)
def test_identifier_is_converted_from_ldap(dag, unique_logical_date):
    """
    Test that the identifier is converted from LDAP data
    :param dag:
    :param unique_logical_date:
    :return:
    """
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_logical_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids=TEST_TASK_ID) == {
        "uid=1234,ou=people,dc=example,dc=org": {
            "identifiers": [
                {
                    "type": "local",
                    "value": "1234"
                },
                {
                    "type": "eduPersonPrincipalName",
                    "value": "1234@example.org"
                },
            ],
        }
    }


@pytest.mark.parametrize("dag", [
    {
        "task_name": TESTED_TASK_NAME,
        "param_names": ["raw_results"],
        "raw_results": {
            "uid=1234,ou=people,dc=example,dc=org": {},
        },
    }
], indirect=True)
def test_missing_identifier(dag, unique_logical_date):
    """
    Test that if the identifier is not present, the value is None.
    :param dag: The DAG object
    :param unique_logical_date: unique execution date
    :return: None
    """
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_logical_date)
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

@pytest.mark.parametrize("dag", [
    {
        "task_name": TESTED_TASK_NAME,
        "param_names": ["raw_results"],
        "raw_results": {
            "uid=1234,ou=people,dc=example,dc=org": {
                "uid": ["1234"],
                "eduPersonPrincipalName": ["invalid-email-format"]
            },
        },
    }
], indirect=True)
def test_invalid_eppn_format(dag, unique_logical_date):
    """
    Test that an invalid eduPersonPrincipalName format results in its exclusion.
    :param dag: The DAG object
    :param unique_logical_date: unique execution date
    :return: None
    """
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_logical_date)
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
            ],
        }
    }

@pytest.mark.parametrize("dag", [
    {
        "task_name": TESTED_TASK_NAME,
        "param_names": ["raw_results"],
        "raw_results": {
            "uid=1234,ou=people,dc=example,dc=org": {
                "uid": ["1234"],
                "eduPersonPrincipalName": ["%user%@example.org"]
            },
        },
    }], indirect=True)
def test_eppn_with_invalid_local_part(dag, unique_logical_date):
    """
    Test that an eduPersonPrincipalName with invalid local part is invalidated.
    :param dag: The DAG object
    :param unique_logical_date: unique execution date
    :return: None
    """
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_logical_date)
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
            ],
        }
    }

@pytest.mark.parametrize("dag", [
    {
        "task_name": TESTED_TASK_NAME,
        "param_names": ["raw_results"],
        "raw_results": {
            "uid=1234,ou=people,dc=example,dc=org": {
                "uid": ["1234"],
                "eduPersonPrincipalName": ["user@example.o"]
            },
        },
    }], indirect=True)
def test_eppn_with_invalid_domain_part(dag, unique_logical_date):
    """
    Test that an eduPersonPrincipalName with invalid domain part is invalidated.
    :param dag: The DAG object
    :param unique_logical_date: unique execution date
    :return: None
    """
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_logical_date)
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
            ],
        }
    }

@pytest.mark.parametrize("dag", [
    {
        "task_name": TESTED_TASK_NAME,
        "param_names": ["raw_results"],
        "raw_results": {
            "uid=1234,ou=people,dc=example,dc=org": {
                "uid": ["1234"],
                "eduPersonPrincipalName": ["user@@example.org"]
            },
        },
    }
], indirect=True)
def test_eppn_with_double_at_symbol(dag, unique_logical_date):
    """
    Test that an eduPersonPrincipalName with double '@' symbol is invalidated.
    :param dag: The DAG object
    :param unique_logical_date: unique execution date
    :return: None
    """
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_logical_date)
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
            ],
        }
    }

@pytest.mark.parametrize("dag", [
    {
        "task_name": TESTED_TASK_NAME,
        "param_names": ["raw_results"],
        "raw_results": {
            "uid=1234,ou=people,dc=example,dc=org": {
                "uid": ["1234"],
                "eduPersonPrincipalName": [12345]
            },
        },
    }
], indirect=True)
def test_eppn_with_invalid_type(dag, unique_logical_date):
    """
    Test that an eduPersonPrincipalName with invalid type is excluded.
    :param dag: The DAG object
    :param unique_logical_date: unique execution date
    :return: None
    """
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_logical_date)
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
            ],
        }
    }
