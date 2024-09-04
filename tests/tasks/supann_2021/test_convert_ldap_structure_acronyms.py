import pytest
from airflow.utils.state import TaskInstanceState

from test_utils.dags import create_dag_run, \
    DATA_INTERVAL_START, \
    DATA_INTERVAL_END, \
    create_task_instance

TESTED_TASK_NAME = "tasks.supann_2021.convert_ldap_structure_acronyms" \
                   ".convert_ldap_structure_acronyms"
TEST_TASK_ID = "convert_ldap_structure_acronyms"


@pytest.mark.parametrize("dag", [
    {
        'task_name': TESTED_TASK_NAME,
        'raw_results': {
            "uid=1234,ou=people,dc=example,dc=org": {
                "acronym": ["UEX"],
                "eduorglegalname": ["University of Example"],
                "description": ["A university in Example"],
            },
        },
    },
], indirect=True)
def test_acronym_is_converted_from_ldap(dag, unique_execution_date) -> None:
    """
    Test that the acronym is converted from the LDAP entry
    :param dag: The DAG object
    :param unique_execution_date: unique execution date
    :return: None
    """
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_execution_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids=TEST_TASK_ID) == {
        "uid=1234,ou=people,dc=example,dc=org": {'acronym': 'UEX'}
    }


@pytest.mark.parametrize("dag", [
    {
        'task_name': TESTED_TASK_NAME,
        'raw_results': {
            "uid=1234,ou=people,dc=example,dc=org": {
                "eduorglegalname": ["University of Example"],
                "description": ["A university in Example"],
            },
        },
    },
], indirect=True)
def test_acronym_is_empty_if_not_present(dag, unique_execution_date) -> None:
    """
    Test that the acronym is empty if not present in the LDAP entry
    :param dag: The DAG object
    :param unique_execution_date: unique execution date
    :return: None
    """
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_execution_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids=TEST_TASK_ID) == {
        "uid=1234,ou=people,dc=example,dc=org": {'acronym': None}
    }
