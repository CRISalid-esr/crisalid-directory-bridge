import pytest

from airflow.utils.state import TaskInstanceState

from test_utils.dags import create_dag_run, \
    create_task_instance, \
    DATA_INTERVAL_START, \
    DATA_INTERVAL_END

TEST_TASK_ID = "convert_spreadsheet_structure"

TESTED_TASK_NAME = 'tasks.spreadsheet.convert_spreadsheet_structure.convert_spreadsheet_structure'


@pytest.mark.parametrize("dag, expected_result", [
    (
            {
                "task_name": TESTED_TASK_NAME,
                "ldap_results": [
                    {
                        'name': 'Laboratoire de gÃ©ographie physique Pierre Birot (UMR 8591)',
                        'acronym': '',
                        'description': 'Laboratoire de gÃ©ographie physique Pierre Birot (UMR 8591)',
                        'local_identifier': 'U082',
                        'RNSR': '199812919F',
                        'city_name': 'MEUDON',
                        'city_code': '92190',
                        'city_adress': 'Centre Meudon, 1 PLACE ARISTIDE BRIAND',
                    }
                ]
            },
            {
                'supannCodeEntite=U082,ou=structures,dc=univ-paris1,dc=fr': {
                    'names': [
                        {
                            'value': 'LGP\xa0: Laboratoire de gÃ©ographie physique Pierre Birot (UMR 8591)',
                            'language': 'fr'
                        }
                    ],
                    'acronym': None,
                    'descriptions': [
                        {
                            'value': 'LGP\xa0: Laboratoire de gÃ©ographie physique Pierre Birot (UMR 8591)',
                            'language': 'fr'
                        }
                    ],
                    'contacts': [
                        {
                            'type': 'postal_address',
                            'format': 'structured_physical_address',
                            'value': {
                                'country': 'France',
                                'zip_code': '92190',
                                'city': 'MEUDON',
                                'street': 'Centre Meudon, 1 PLACE ARISTIDE BRIAND'
                            }
                        }
                    ],
                    'identifiers': [
                        {
                            'type': 'local',
                            'value': 'U082'
                        },
                        {
                            'type': 'RNSR',
                            'value': '199812919F'
                        }
                    ]
                }
            }
    ),
], indirect=["dag"])
def test_convert_spreadsheet_structure(dag, expected_result, unique_execution_date):
    """
    Test that the csv data are converted to the expected format
    """
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_execution_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids=TEST_TASK_ID) == expected_result
