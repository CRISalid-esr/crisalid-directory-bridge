import pytest
from airflow.utils.state import TaskInstanceState

from test_utils.dags import create_dag_run, DATA_INTERVAL_START, DATA_INTERVAL_END, create_task_instance

TEST_TASK_ID = "override_ldap_structure_data"
TESTED_TASK_NAME = "tasks.supann_2021.override_ldap_structure_data.override_ldap_structure_data"


def _params(ldap_source, spreadsheet_source):
    return {
        "task_name": TESTED_TASK_NAME,
        "param_names": ["ldap_source", "spreadsheet_source"],
        "ldap_source": ldap_source,
        "spreadsheet_source": spreadsheet_source,
    }


@pytest.mark.parametrize("dag", [_params(
    ldap_source={"U1": {"generic_type": "unit", "main_mission": "research"}},
    spreadsheet_source={},
)], indirect=True)
def test_ldap_only_entry_kept(dag, unique_logical_date):
    """An LDAP entry with no CSV counterpart is kept unchanged."""
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_logical_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SUCCESS
    result = ti.xcom_pull(task_ids=TEST_TASK_ID)
    assert result == {"U1": {"generic_type": "unit", "main_mission": "research"}}


@pytest.mark.parametrize("dag", [_params(
    ldap_source={},
    spreadsheet_source={"CSV1": {"generic_type": "unit", "type": "UMR"}},
)], indirect=True)
def test_csv_only_entry_added(dag, unique_logical_date):
    """A CSV entry with no LDAP counterpart is added to the result."""
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_logical_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SUCCESS
    result = ti.xcom_pull(task_ids=TEST_TASK_ID)
    assert result == {"CSV1": {"generic_type": "unit", "type": "UMR"}}


@pytest.mark.parametrize("dag", [_params(
    ldap_source={"U1": {"generic_type": "unit", "main_mission": "research", "type": None}},
    spreadsheet_source={"U1": {"type": "UMR"}},
)], indirect=True)
def test_nonempty_csv_field_overwrites_ldap(dag, unique_logical_date):
    """A non-empty CSV field value overwrites the LDAP value."""
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_logical_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SUCCESS
    result = ti.xcom_pull(task_ids=TEST_TASK_ID)
    assert result["U1"]["type"] == "UMR"
    assert result["U1"]["main_mission"] == "research"


@pytest.mark.parametrize("dag", [_params(
    ldap_source={"U1": {"generic_type": "unit", "main_mission": "research"}},
    spreadsheet_source={"U1": {"main_mission": ""}},
)], indirect=True)
def test_empty_csv_field_preserves_ldap(dag, unique_logical_date):
    """An empty CSV field value does not overwrite the LDAP value."""
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_logical_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SUCCESS
    result = ti.xcom_pull(task_ids=TEST_TASK_ID)
    assert result["U1"]["main_mission"] == "research"


@pytest.mark.parametrize("dag", [_params(
    ldap_source={"U1": {
        "generic_type": "unit",
        "identifiers": [{"type": "local", "value": "U1"}],
    }},
    spreadsheet_source={"U1": {
        "identifiers": [{"type": "local", "value": "U1"}, {"type": "ror", "value": "03abc1234"}],
    }},
)], indirect=True)
def test_nested_field_replaced_by_csv(dag, unique_logical_date):
    """A non-empty CSV list field replaces the LDAP list (field-level override)."""
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_logical_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SUCCESS
    result = ti.xcom_pull(task_ids=TEST_TASK_ID)
    identifiers = result["U1"]["identifiers"]
    assert len(identifiers) == 2
    assert any(i["type"] == "ror" for i in identifiers)


@pytest.mark.parametrize("dag", [_params(
    ldap_source={},
    spreadsheet_source={},
)], indirect=True)
def test_empty_inputs_produce_empty_output(dag, unique_logical_date):
    """Both empty dicts → empty output."""
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_logical_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids=TEST_TASK_ID) == {}
