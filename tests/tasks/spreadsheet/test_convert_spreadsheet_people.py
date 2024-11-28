import json

import pytest
from airflow.utils.state import TaskInstanceState

from test_utils.dags import create_dag_run, \
    create_task_instance, \
    DATA_INTERVAL_START, \
    DATA_INTERVAL_END

TEST_TASK_ID = "convert_spreadsheet_people"

TESTED_TASK_NAME = 'tasks.spreadsheet.convert_spreadsheet_people.convert_spreadsheet_people'


@pytest.mark.parametrize("dag, expected_result_path", [
    (
            {
                "task_name": TESTED_TASK_NAME,
                "param_names": ["raw_results"],
                "raw_results": [
                    {
                        'first_name': 'Joe',
                        'last_name': 'Dupond',
                        'main_laboratory_identifier': 'U01',
                        'tracking_id': 'jdupond',
                        'id_hal_i': '',
                        'id_hal_s': '',
                        'orcid': '0000-0000-0000-0001',
                        'idref': '12345678X',
                        'scopus_eid': '',
                    }
                ]
            },
            "./tests/data/test_convert_spreadsheet_people.json"
    ),
    (
            {
                "task_name": TESTED_TASK_NAME,
                "param_names": ["raw_results"],
                "raw_results": [
                    {
                        'first_name': 'Henry',
                        'last_name': 'Gerald',
                        'main_laboratory_identifier': 'U85',
                        'tracking_id': 'hgerald',
                        'id_hal_i': '',
                        'id_hal_s': '',
                        'orcid': '',
                        'idref': '',
                        'scopus_eid': '',
                    }
                ]
            },
            "./tests/data/test_convert_spreadsheet_people_with_local_identifier_only.json"
    ),
    (
            {
                "task_name": TESTED_TASK_NAME,
                "param_names": ["raw_results"],
                "raw_results": [
                    {
                        'first_name': 'Henry',
                        'last_name': 'Gerald',
                        'main_laboratory_identifier': 'U85',
                        'tracking_id': 'hgerald',
                        'id_hal_i': '054235',
                        'id_hal_s': 'henry-gerald',
                        'orcid': '0000-0000-0000-0001',
                        'idref': '012345678X',
                        'scopus_eid': '5432345678X',
                    },
                    {
                        'first_name': 'Joe',
                        'last_name': 'Dupond',
                        'main_laboratory_identifier': 'U01',
                        'tracking_id': 'jdupond',
                        'id_hal_i': '',
                        'id_hal_s': '',
                        'orcid': '',
                        'idref': '',
                        'scopus_eid': '',
                    }
                ]
            },
            "./tests/data/test_convert_spreadsheet_with_two_people.json"
    ),
],
                         indirect=["dag"],
                         ids=[
                             "test_convert_spreadsheet_people",
                             "test_convert_spreadsheet_people_with_local_identifier_only",
                             "test_convert_spreadsheet_with_two_people",
                         ]
                         )
def test_convert_spreadsheet_people(dag, expected_result_path, unique_execution_date):
    """
    Test that the csv data are converted to the expected format
    """
    # pylint: disable=duplicate-code
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_execution_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)

    with open(expected_result_path, 'r', encoding='utf-8') as f:
        expected_result = json.load(f)
    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids=TEST_TASK_ID) == expected_result
