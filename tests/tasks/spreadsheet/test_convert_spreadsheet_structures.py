import json

import pytest
from airflow.utils.state import TaskInstanceState

from test_utils.dags import create_dag_run, \
    create_task_instance, \
    DATA_INTERVAL_START, \
    DATA_INTERVAL_END

TEST_TASK_ID = "convert_spreadsheet_structures"

TESTED_TASK_NAME = 'tasks.spreadsheet.convert_spreadsheet_structures.convert_spreadsheet_structures'


@pytest.mark.parametrize("dag, expected_result_path", [
    (
            {
                "task_name": TESTED_TASK_NAME,
                "param_names": ["raw_results"],
                "raw_results": [
                    {
                        'name': 'Laboratoire de géographie physique Pierre Birot (UMR 8591)',
                        'acronym': '',
                        'description': 'Lab description',
                        'tracking_id': 'U082',
                        'nns': '199812919F',
                        'ror': '',
                        'scopus_id': '60112301',
                        'hal_collection': 'https://paris1.hal.science/LGP',
                        'city_name': 'MEUDON',
                        'city_code': '92190',
                        'city_adress': 'Centre Meudon, 1 PLACE ARISTIDE BRIAND',
                    }
                ]
            },
            "./tests/data/test_convert_structure_without_acronym.json"
    ),
    (
            {
                "task_name": TESTED_TASK_NAME,
                "param_names": ["raw_results"],
                "raw_results": [
                    {
                        'name': 'Laboratoire des Tests (UMR 2024)',
                        'acronym': 'TEST',
                        'description': 'Laboratoire des Tests (UMR 2024)',
                        'tracking_id': 'U086',
                        'nns': '123456789F',
                        'ror': '567890123',
                        'scopus_id': '60112301',
                        'hal_collection': 'https://paris1.hal.science/LGP',
                        'city_name': 'MEUDON',
                        'city_code': '92190',
                        'city_adress': 'Centre Meudon, 1 PLACE ARISTIDE BRIAND',
                    }
                ]
            },
            "./tests/data/test_convert_structure_with_acronym.json"
    ),
    (
            {
                "task_name": TESTED_TASK_NAME,
                "param_names": ["raw_results"],
                "raw_results": [
                    {
                        'name': 'Laboratoire des Tests (UMR 2024)',
                        'acronym': 'TEST',
                        'description': 'Laboratoire des Tests (UMR 2024)',
                        'tracking_id': 'U086',
                        'nns': '123456789F',
                        'ror': '567890123',
                        'scopus_id': '60112301',
                        'hal_collection': 'https://paris1.hal.science/LGP',
                        'city_name': 'MEUDON',
                        'city_code': '92190',
                        'city_adress': 'Centre Meudon, 1 PLACE ARISTIDE BRIAND',
                    },
                    {
                        'name': 'Laboratoire de géographie physique Pierre Birot (UMR 8591)',
                        'acronym': '',
                        'description': 'test geolab',
                        'tracking_id': 'U082',
                        'nns': '199812919F',
                        'ror': '',
                        'scopus_id': '60112301',
                        'hal_collection': 'https://paris1.hal.science/LGP',
                        'city_name': 'MEUDON',
                        'city_code': '92190',
                        'city_adress': 'Centre Meudon, 1 PLACE ARISTIDE BRIAND',
                    }
                ]
            },
            "./tests/data/test_convert_with_two_structures.json"
    ),
],
                         indirect=["dag"],
                         ids=[
                             "test_convert_structure_without_acronym",
                             "test_convert_structure_with_acronym",
                             "test_convert_with_two_structures"
                         ]
                         )
def test_convert_spreadsheet_structures(dag, expected_result_path, unique_logical_date):
    """
    Test that the csv data are converted to the expected format
    """
    # pylint: disable=duplicate-code
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_logical_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    with open(expected_result_path, 'r', encoding='utf-8') as f:
        expected_result = json.load(f)
    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids=TEST_TASK_ID) == expected_result
