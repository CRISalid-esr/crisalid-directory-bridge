import json

import pytest
from _pytest.fixtures import SubRequest
from airflow import DAG
from airflow.utils.state import TaskInstanceState

from test_utils.dags import create_dag_run, \
    create_task_instance, \
    DATA_INTERVAL_START, \
    DATA_INTERVAL_END, TEST_DAG_ID
from utils.dependencies import import_from_path

TEST_TASK_ID = "complete_identifiers"

TESTED_TASK_NAME = 'tasks.supann_2021.complete_identifiers.complete_identifiers'


@pytest.fixture(name="dag")
def dag_fixture(request: SubRequest) -> DAG:
    """
    Create a DAG for testing the convert_ldap_structure_description_task
    :param request: The pytest request object
    :return: The DAG object
    """
    task_name = request.param['task_name']
    ldap_source = request.param['ldap_source']
    identifiers_spreadsheet = request.param['identifiers_spreadsheet']
    with DAG(
            dag_id=TEST_DAG_ID,
            schedule="@daily",
            start_date=DATA_INTERVAL_START,
    ) as created_dag:
        convert_description_task = import_from_path(task_name)
        convert_description_task(ldap_source, identifiers_spreadsheet)
    return created_dag


@pytest.mark.parametrize("dag, expected_result_path", [
    (
            {
                "task_name": TESTED_TASK_NAME,
                "ldap_source": {
                    "jdupond": {
                        "names": [
                            {
                                "last_names": [
                                    {
                                        "value": "Dupond",
                                        "language": "fr"
                                    }
                                ],
                                "first_names": [
                                    {
                                        "value": "Joe",
                                        "language": "fr"
                                    }
                                ]
                            }
                        ],
                        "identifiers": [
                            {
                                "type": "local",
                                "value": "jdupond"
                            }
                        ],
                        "memberships": [
                            {
                                "entity_id": "U01"
                            }
                        ]
                    },
                    "hgerald": {
                        "names": [
                            {
                                "last_names": [
                                    {
                                        "value": "Gerald",
                                        "language": "fr"
                                    }
                                ],
                                "first_names": [
                                    {
                                        "value": "Henry",
                                        "language": "fr"
                                    }
                                ]
                            }
                        ],
                        "identifiers": [
                            {
                                "type": "local",
                                "value": "hgerald"
                            }
                        ],
                        "memberships": [
                            {
                                "entity_id": "U01"
                            }
                        ]
                    }
                },
                "identifiers_spreadsheet": [
                    {'local': 'loc1', 'idref': '1234', 'orcid': '0000-0001-2345-6789'},
                    {'local': 'jdupond', 'idref': "12345678X", 'orcid': '0000-0000-0000-0001'},
                    {'local': 'hgerald', 'idref': '9101', 'orcid': '0000-0003-4567-8901'}
                ]
            },
            "./tests/data/test_complete_identifiers_with_two_entities.json"
    ),
    (
            {
                "task_name": TESTED_TASK_NAME,
                "ldap_source": {
                    "jdupond": {
                        "names": [
                            {
                                "last_names": [
                                    {
                                        "value": "Dupond",
                                        "language": "fr"
                                    }
                                ],
                                "first_names": [
                                    {
                                        "value": "Joe",
                                        "language": "fr"
                                    }
                                ]
                            }
                        ],
                        "identifiers": [
                            {
                                "type": "local",
                                "value": "jdupond"
                            }
                        ],
                        "memberships": [
                            {
                                "entity_id": "U01"
                            }
                        ]
                    }
                },
                "identifiers_spreadsheet": [
                    {'local': 'loc1', 'idref': '1234', 'orcid': '0000-0001-2345-6789'},
                    {'local': 'jdupond', 'idref': "12345678X", 'orcid': '0000-0000-0000-0001'},
                    {'local': 'jdupond', 'idref': "12345678X", 'orcid': '0000-0000-0000-0001'},
                    {'local': 'hgerald', 'idref': '9101', 'orcid': '0000-0003-4567-8901'}
                ]
            },
            "./tests/data/test_complete_identifiers_with_one_entity.json"
    ),
    (
            {
                "task_name": TESTED_TASK_NAME,
                "ldap_source": {
                    "jdupond": {
                        "names": [
                            {
                                "last_names": [
                                    {
                                        "value": "Dupond",
                                        "language": "fr"
                                    }
                                ],
                                "first_names": [
                                    {
                                        "value": "Joe",
                                        "language": "fr"
                                    }
                                ]
                            }
                        ],
                        "identifiers": [
                            {
                                "type": "local",
                                "value": "jdupond"
                            },
                            {
                                "type": "idref",
                                "value": "12345678X"
                            }
                        ],
                        "memberships": [
                            {
                                "entity_id": "U01"
                            }
                        ]
                    },
                },
                "identifiers_spreadsheet": [
                    {'local': 'loc1', 'idref': '1234', 'orcid': '0000-0001-2345-6789'},
                    {'local': 'jdupond', 'idref': "12345678X", 'orcid': '0000-0000-0000-0001'},
                    {'local': 'hgerald', 'idref': '9101', 'orcid': '0000-0003-4567-8901'}
                ]
            },
            "./tests/data/test_complete_identifiers_with_one_entity.json"
    ),
    (
            {
                "task_name": TESTED_TASK_NAME,
                "ldap_source": {
                    "jdupond": {
                        "names": [
                            {
                                "last_names": [
                                    {
                                        "value": "Dupond",
                                        "language": "fr"
                                    }
                                ],
                                "first_names": [
                                    {
                                        "value": "Joe",
                                        "language": "fr"
                                    }
                                ]
                            }
                        ],
                        "identifiers": [
                            {
                                "type": "local",
                                "value": "jdupond"
                            },
                            {
                                "type": "idref",
                                "value": "12345678X"
                            },
                            {
                                "type": "idref",
                                "value": "12345678X"
                            }
                        ],
                        "memberships": [
                            {
                                "entity_id": "U01"
                            }
                        ]
                    },
                },
                "identifiers_spreadsheet": [
                    {'local': 'loc1', 'idref': '1234', 'orcid': '0000-0001-2345-6789'},
                    {'local': 'jdupond', 'idref': "12345678X", 'orcid': '0000-0000-0000-0001'},
                    {'local': 'hgerald', 'idref': '9101', 'orcid': '0000-0003-4567-8901'}
                ]
            },
            "./tests/data/test_complete_identifiers_with_one_entity.json"
    ),
],
                         indirect=["dag"],
                         ids=[
                             "test_complete_identifiers_with_two_entities",
                             "test_complete_identifiers_with_dupe_identifier_list",
                             "test_complete_identifiers_with_known_idref",
                             "test_complete_identifiers_with_dupe_identifier_in_ldap",
                         ]
                         )
def test_complete_identifiers(dag, expected_result_path, unique_execution_date):
    """
    temp
    """
    dag_run = create_dag_run(dag, DATA_INTERVAL_START,
                             DATA_INTERVAL_END, unique_execution_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)

    with open(expected_result_path, 'r', encoding='utf-8') as f:
        expected_result = json.load(f)
    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids=TEST_TASK_ID) == expected_result
