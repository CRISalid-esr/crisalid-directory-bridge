import pytest
from airflow.utils.state import TaskInstanceState

from test_utils.dags import create_dag_run, \
    DATA_INTERVAL_START, \
    DATA_INTERVAL_END, \
    create_task_instance

TESTED_TASK_NAME = ("tasks.supann_2021.convert_ldap_structure_relationships"
                    ".convert_ldap_structure_relationships")
TEST_TASK_ID = "convert_ldap_structure_relationships"

DN = "uid=U031,ou=structures,dc=example,dc=org"


@pytest.mark.parametrize("dag", [
    {
        "task_name": TESTED_TASK_NAME,
        "param_names": ["raw_results"],
        "raw_results": {
            DN: {
                "ou": ["UMR 7041 - ARSCAN"],
                "supannCodeEntiteParent": ["FAC-SCI-001"],
            }
        },
    }
], indirect=True)
def test_parent_code_produces_part_of_relationship(dag, unique_logical_date) -> None:
    """supannCodeEntiteParent XYZ → part_of relationship to local-XYZ."""
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_logical_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids=TEST_TASK_ID) == {
        DN: {"relationships": [
            {"type": "part_of", "subtype": None,
             "target": "local-FAC-SCI-001",
             "start_date": None, "end_date": None}
        ]}
    }


@pytest.mark.parametrize("dag", [
    {
        "task_name": TESTED_TASK_NAME,
        "param_names": ["raw_results"],
        "raw_results": {
            DN: {"ou": ["UMR 7041 - ARSCAN"]}
        },
    }
], indirect=True)
def test_missing_parent_produces_empty_relationships(dag, unique_logical_date) -> None:
    """No supannCodeEntiteParent → empty relationships list."""
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_logical_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids=TEST_TASK_ID) == {
        DN: {"relationships": []}
    }
