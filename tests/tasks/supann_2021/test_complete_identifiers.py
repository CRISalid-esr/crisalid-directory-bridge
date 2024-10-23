import json

import pytest
from airflow.utils.state import TaskInstanceState

from test_utils.dags import create_dag_run, \
    create_task_instance, \
    DATA_INTERVAL_START, \
    DATA_INTERVAL_END

TEST_TASK_ID = "complete_identifiers"

TESTED_TASK_NAME = 'tasks.supann_2021.complete_identifiers.complete_identifiers'


@pytest.mark.parametrize("dag, expected_result_path", [
    (
            {
                "task_name": TESTED_TASK_NAME,
                "param_names": ["ldap_source", "identifiers_spreadsheet"],
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
                                "entity_uid": "U01"
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
                                "entity_uid": "U01"
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
                "param_names": ["ldap_source", "identifiers_spreadsheet"],
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
                                "entity_uid": "U01"
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
                "param_names": ["ldap_source", "identifiers_spreadsheet"],
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
                                "entity_uid": "U01"
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
                "param_names": ["ldap_source", "identifiers_spreadsheet"],
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
                                "entity_uid": "U01"
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
    Test that the identifiers from ldap are completed with the spreadsheet data
    """
    # pylint: disable=duplicate-code
    dag_run = create_dag_run(dag, DATA_INTERVAL_START,
                             DATA_INTERVAL_END, unique_execution_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    with open(expected_result_path, 'r', encoding='utf-8') as f:
        expected_result = json.load(f)
    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids=TEST_TASK_ID) == expected_result
