import pytest
from airflow.utils.state import TaskInstanceState

from test_utils.dags import create_dag_run, \
    DATA_INTERVAL_START, \
    DATA_INTERVAL_END, \
    create_task_instance

TESTED_TASK_NAME = 'tasks.supann_2021.convert_ldap_structure_descriptions' \
                   '.convert_ldap_structure_descriptions'
TEST_TASK_ID = "convert_ldap_structure_descriptions"


@pytest.mark.parametrize("dag", [
    {
        'task_name': TESTED_TASK_NAME,
        'ldap_results': {
            "uid=3456,ou=people,dc=example,dc=org": {
                "description": ["A university in Example 1"],
                "eduorglegalname": ["University of Example 1"],
            },
            "uid=6789,ou=people,dc=example,dc=org": {
                "eduorglegalname": ["Another University 3"],
            }
        }
    }
], indirect=True)
def test_description_is_converted_from_ldap(dag, unique_execution_date) -> None:
    """
    Test that the description is converted from the LDAP entry
    :param dag: The DAG object
    :param unique_execution_date: Unique execution date
    :return: None
    """
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_execution_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    expected_results = {
        "uid=3456,ou=people,dc=example,dc=org": {
            "descriptions": [{"language": "fr", "value": "A university in Example 1"}]},
        "uid=6789,ou=people,dc=example,dc=org": {
            "descriptions": []
        }
    }

    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids=TEST_TASK_ID) == expected_results
