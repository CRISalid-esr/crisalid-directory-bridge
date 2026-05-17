import pytest
from airflow.utils.state import TaskInstanceState

from test_utils.dags import create_dag_run, \
    DATA_INTERVAL_START, \
    DATA_INTERVAL_END, \
    create_task_instance

TESTED_TASK_NAME = ("tasks.supann_2021.convert_ldap_structure_type"
                    ".convert_ldap_structure_type")
TEST_TASK_ID = "convert_ldap_structure_type"

DN = "uid=U031,ou=structures,dc=example,dc=org"


def _make_dag_params(business_category=None):
    ldap_entry = {"ou": ["UMR 7041 - ARSCAN"]}
    if business_category is not None:
        ldap_entry["businessCategory"] = [business_category]
    return {
        "task_name": TESTED_TASK_NAME,
        "param_names": ["raw_results"],
        "raw_results": {DN: ldap_entry},
    }


@pytest.mark.parametrize("dag", [_make_dag_params("research")], indirect=True)
def test_research_maps_to_research(dag, unique_logical_date) -> None:
    """businessCategory 'research' → main_mission 'research'."""
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_logical_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids=TEST_TASK_ID) == {
        DN: {"generic_type": "unit", "main_mission": "research"}
    }


@pytest.mark.parametrize("dag", [_make_dag_params("administration")], indirect=True)
def test_administration_maps_to_administrative_services(dag, unique_logical_date) -> None:
    """businessCategory 'administration' → main_mission 'administrative_services'."""
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_logical_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids=TEST_TASK_ID) == {
        DN: {"generic_type": "unit", "main_mission": "administrative_services"}
    }


@pytest.mark.parametrize("dag", [_make_dag_params("library")], indirect=True)
def test_library_maps_to_scientific_services(dag, unique_logical_date) -> None:
    """businessCategory 'library' → main_mission 'scientific_services'."""
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_logical_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids=TEST_TASK_ID) == {
        DN: {"generic_type": "unit", "main_mission": "scientific_services"}
    }


@pytest.mark.parametrize("dag", [_make_dag_params("pedagogy")], indirect=True)
def test_pedagogy_maps_to_learning(dag, unique_logical_date) -> None:
    """businessCategory 'pedagogy' → main_mission 'learning'."""
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_logical_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids=TEST_TASK_ID) == {
        DN: {"generic_type": "unit", "main_mission": "learning"}
    }


@pytest.mark.parametrize("dag", [_make_dag_params("organization")], indirect=True)
def test_organization_omits_main_mission(dag, unique_logical_date) -> None:
    """businessCategory 'organization' → main_mission omitted."""
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_logical_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SUCCESS
    result = ti.xcom_pull(task_ids=TEST_TASK_ID)
    assert result == {DN: {"generic_type": "unit"}}
    assert "main_mission" not in result[DN]


@pytest.mark.parametrize("dag", [_make_dag_params()], indirect=True)
def test_missing_business_category_omits_main_mission(dag, unique_logical_date) -> None:
    """No businessCategory in LDAP entry → main_mission omitted."""
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_logical_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SUCCESS
    result = ti.xcom_pull(task_ids=TEST_TASK_ID)
    assert result == {DN: {"generic_type": "unit"}}
    assert "main_mission" not in result[DN]


@pytest.mark.parametrize("dag", [_make_dag_params("research")], indirect=True)
def test_generic_type_uses_env_variable(dag, unique_logical_date, monkeypatch) -> None:
    """LDAP_STRUCTURE_GENERIC_TYPE env variable controls generic_type."""
    monkeypatch.setenv("LDAP_STRUCTURE_GENERIC_TYPE", "institution")
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_logical_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids=TEST_TASK_ID) == {
        DN: {"generic_type": "institution", "main_mission": "research"}
    }
