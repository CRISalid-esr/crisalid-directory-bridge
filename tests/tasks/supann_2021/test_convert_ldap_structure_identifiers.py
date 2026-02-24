import pytest
from airflow.utils.state import TaskInstanceState

from test_utils.dags import create_dag_run, \
    DATA_INTERVAL_START, \
    DATA_INTERVAL_END, \
    create_task_instance

TESTED_TASK_NAME = 'tasks.supann_2021.convert_ldap_structure_identifiers' \
                   '.convert_ldap_structure_identifiers'

TEST_TASK_ID = "convert_ldap_structure_identifiers"


@pytest.mark.parametrize("dag", [
    {
        'task_name': TESTED_TASK_NAME,
        "param_names": ["raw_results"],
        'raw_results': {
            "uid=1234,ou=people,dc=example,dc=org": {
                "supannCodeEntite": ["123456"],
                "supannRefId": ["{RNSR}654321"],
                "eduorglegalname": ["University of Example"],
                "description": ["A university in Example"],
            },
            "uid=5678,ou=people,dc=example,dc=org": {
                "supannCodeEntite": ["789012"],
                "eduorglegalname": ["Another University"],
                "description": ["Another university"],
            },
        },
    }
], indirect=True)
def test_identifiers_are_converted_from_ldap(dag, unique_logical_date):
    """
    Test that identifiers are retrieved from LDAP
    :param dag:
    :param unique_logical_date:
    :return:
    """
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_logical_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    expected_results = {
        "uid=1234,ou=people,dc=example,dc=org": {
            "identifiers": [{"type": "local", "value": "123456"},
                            {"type": "nns", "value": "654321"}]},
        "uid=5678,ou=people,dc=example,dc=org": {
            "identifiers": [{"type": "local", "value": "789012"}]}
    }
    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids=TEST_TASK_ID) == expected_results


@pytest.mark.parametrize("dag", [
    {
        'task_name': TESTED_TASK_NAME,
        "param_names": ["raw_results"],
        'raw_results': {
            "uid=91011,ou=people,dc=example,dc=org": {
                "supannRefId": ["{RNSR}112233"],
                "eduorglegalname": ["Missing Entity University"],
                "description": ["A university with missing supannCodeEntite"],
            },
        },
    }
], indirect=True)
def test_missing_supann_code_entite(dag, unique_logical_date):
    """
    Test that the task fails if the supannCodeEntite is missing
    :param dag: The DAG object
    :param unique_logical_date: The unique execution date
    :return:
    """
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_logical_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)

    with pytest.raises(AssertionError, match="missing supannCodeEntite"):
        ti.run(ignore_ti_state=True)
