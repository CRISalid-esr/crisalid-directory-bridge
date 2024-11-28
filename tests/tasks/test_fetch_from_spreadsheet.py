import json

import pytest
from airflow.utils.state import TaskInstanceState

from test_utils.dags import create_dag_run, \
    create_task_instance, \
    DATA_INTERVAL_START, \
    DATA_INTERVAL_END

TEST_TASK_ID = "fetch_from_spreadsheet"

TESTED_TASK_NAME = 'tasks.fetch_from_spreadsheet.fetch_from_spreadsheet'


@pytest.mark.parametrize("dag, expected_result_path", [
    (
            {
                "task_name": TESTED_TASK_NAME,
                "param_names": ["entity_source", "entity_type"],
                "entity_source": "ldap",
                "entity_type": "people",
            },
            "./tests/data/test_data_from_people_csv.json"
    ),
],
                         indirect=["dag"],
                         ids=[
                             "test_fetch_from_spread_sheet"
                         ]
                         )
def test_fetch_from_spread_sheet(dag, expected_result_path, unique_execution_date, monkeypatch):
    """
    Test that data is fetched from a spreadsheet and returned as a list of dictionaries.
    """
    monkeypatch.setenv("PEOPLE_SPREADSHEET_PATH", "./tests/data/csv/people.csv")
    dag_run = create_dag_run(dag, DATA_INTERVAL_START,
                             DATA_INTERVAL_END, unique_execution_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    with open(expected_result_path, 'r', encoding='utf-8') as f:
        expected_result = json.load(f)
    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids=TEST_TASK_ID) == expected_result
