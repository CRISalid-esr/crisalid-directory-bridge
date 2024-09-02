import pytest

from airflow.utils.state import TaskInstanceState

from test_utils.dags import create_dag_run, \
    create_task_instance, \
    DATA_INTERVAL_START, \
    DATA_INTERVAL_END

TEST_TASK_ID = "convert_spreadsheet_people"

TESTED_TASK_NAME = 'tasks.spreadsheet.convert_spreadsheet_people.convert_spreadsheet_people'


@pytest.mark.parametrize("dag, expected_result", [
    (
            {
                "task_name": TESTED_TASK_NAME,
                "ldap_results": [
                    {
                        'first_name': 'Joe',
                        'last_name': 'Dupond',
                        'main_laboratory_supann': 'U01',
                        'local_identifier': 'jdupond',
                        'orcid': '0000-0000-0000-0001',
                        'idref': '12345678X',
                    }
                ]
            },
            {
                'uid=jdupond,ou=people,dc=univ-paris1,dc=fr': {
                    'names': [
                        {'last_names': [{'value': 'Dupond', 'language': 'fr'}],
                         'first_names': [{'value': 'Joe', 'language': 'fr'}]}],
                    'identifiers': [
                        {'type': 'local', 'value': 'jdupond'},
                        {'type': 'orcid', 'value': '0000-0000-0000-0001'},
                        {'type': 'idref', 'value': '12345678X'},
                    ],
                    'memberships': [{'entity': 'U01'}]
                }
            }
    ),
    (
            {
                "task_name": TESTED_TASK_NAME,
                "ldap_results": [
                    {
                        'first_name': 'Henry',
                        'last_name': 'Gerald',
                        'main_laboratory_supann': 'U85',
                        'local_identifier': 'hgerald',
                        'orcid': '',
                        'idref': '',
                    }
                ]
            },
            {
                'uid=hgerald,ou=people,dc=univ-paris1,dc=fr': {
                    'names': [
                        {'last_names': [{'value': 'Gerald', 'language': 'fr'}],
                         'first_names': [{'value': 'Henry', 'language': 'fr'}]}],
                    'identifiers': [
                        {'type': 'local', 'value': 'hgerald'},
                        {'type': 'orcid', 'value': ''},
                        {'type': 'idref', 'value': ''},
                    ],
                    'memberships': [{'entity': 'U85'}]
                }
            }
    ),
    (
            {
                "task_name": TESTED_TASK_NAME,
                "ldap_results": [
                    {
                        'first_name': 'Henry',
                        'last_name': 'Gerald',
                        'main_laboratory_supann': 'U85',
                        'local_identifier': 'hgerald',
                        'orcid': '',
                        'idref': '',
                    },
                    {
                        'first_name': 'Joe',
                        'last_name': 'Dupond',
                        'main_laboratory_supann': 'U01',
                        'local_identifier': 'jdupond',
                        'orcid': '0000-0000-0000-0001',
                        'idref': '12345678X',
                    }
                ]
            },
            {
                'uid=hgerald,ou=people,dc=univ-paris1,dc=fr': {
                    'names': [
                        {'last_names': [{'value': 'Gerald', 'language': 'fr'}],
                         'first_names': [{'value': 'Henry', 'language': 'fr'}]}],
                    'identifiers': [
                        {'type': 'local', 'value': 'hgerald'},
                        {'type': 'orcid', 'value': ''},
                        {'type': 'idref', 'value': ''},
                    ],
                    'memberships': [{'entity': 'U85'}]
                },
                'uid=jdupond,ou=people,dc=univ-paris1,dc=fr': {
                    'names': [
                        {'last_names': [{'value': 'Dupond', 'language': 'fr'}],
                         'first_names': [{'value': 'Joe', 'language': 'fr'}]}],
                    'identifiers': [
                        {'type': 'local', 'value': 'jdupond'},
                        {'type': 'orcid', 'value': '0000-0000-0000-0001'},
                        {'type': 'idref', 'value': '12345678X'},
                    ],
                    'memberships': [{'entity': 'U01'}]
                }

            }
    )
], indirect=["dag"])
def test_convert_spreadsheet_people(dag, expected_result, unique_execution_date):
    """
    Test that the csv data are converted to the expected format
    """
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_execution_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids=TEST_TASK_ID) == expected_result
