import pytest
from airflow.utils.state import TaskInstanceState

from test_utils.dags import create_dag_run, \
    DATA_INTERVAL_START, \
    DATA_INTERVAL_END, \
    create_task_instance

TESTED_TASK_NAME = ("tasks.supann_2021.convert_ldap_structure_generic_type"
                    ".convert_ldap_structure_generic_type")
TEST_TASK_ID = "convert_ldap_structure_generic_type"

SAMPLE_LDAP_RESULTS = {
    "uid=U031,ou=structures,dc=example,dc=org": {
        "ou": ["UMR 7041 - ARSCAN"],
        "description": ["ARSCAN : Laboratoire des Tests"],
    }
}


@pytest.mark.parametrize("dag", [
    {
        "task_name": TESTED_TASK_NAME,
        "param_names": ["raw_results"],
        "raw_results": SAMPLE_LDAP_RESULTS,
    }
], indirect=True)
def test_generic_type_defaults_to_unit(dag, unique_logical_date) -> None:
    """Test that generic_type defaults to 'unit' when no env variable is set."""
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_logical_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids=TEST_TASK_ID) == {
        "uid=U031,ou=structures,dc=example,dc=org": {"generic_type": "unit"}
    }


@pytest.mark.parametrize("dag", [
    {
        "task_name": TESTED_TASK_NAME,
        "param_names": ["raw_results"],
        "raw_results": SAMPLE_LDAP_RESULTS,
    }
], indirect=True)
def test_generic_type_uses_env_variable(dag, unique_logical_date, monkeypatch) -> None:
    """Test that generic_type is read from LDAP_STRUCTURE_GENERIC_TYPE env variable."""
    monkeypatch.setenv("LDAP_STRUCTURE_GENERIC_TYPE", "institution")
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_logical_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids=TEST_TASK_ID) == {
        "uid=U031,ou=structures,dc=example,dc=org": {"generic_type": "institution"}
    }