# pylint: disable=duplicate-code

import logging
import pytest
from airflow.utils.state import TaskInstanceState

from test_utils.dags import create_dag_run, \
    create_task_instance, \
    DATA_INTERVAL_START, \
    DATA_INTERVAL_END

TEST_TASK_ID = "convert_ldap_people_employment"

TESTED_TASK_NAME = "tasks.supann_2021.convert_ldap_people_employment.convert_ldap_people_employment"


@pytest.mark.parametrize("dag", [
    {
        "task_name": TESTED_TASK_NAME,
        "param_names": ["raw_results", "local_value_position_dict"],
        "raw_results": {
            'uid=hdupont,ou=people,dc=univ-paris1,dc=fr': {
                'employeeType': ['Professeur des universités'],
                'supannEtablissement': ['{UAI}0753364Z'],
            },
        },
        "local_value_position_dict": {
            "Professeur des universités": ("PR", "Professeur")
        },
    }
], indirect=True)
def test_usual_case(dag, unique_logical_date):
    """
    Test that the employment are converted from the LDAP entry when only employeeType and
    supannEtablissement are known
    :param dag: The DAG object
    :param unique_logical_date: unique execution date
    :return: None
    """
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_logical_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids=TEST_TASK_ID) == {
        'uid=hdupont,ou=people,dc=univ-paris1,dc=fr': {
            "employments": [
                {
                    "position": {
                        "title": "Professeur",
                        "code": "PR"
                    },
                    "main_research_structure": "UAI-0753364Z",
                }
            ]
        }
    }


@pytest.mark.parametrize("dag", [
    {
        "task_name": TESTED_TASK_NAME,
        "param_names": ["raw_results", "local_value_position_dict"],
        "raw_results": {
            'uid=hdupont,ou=people,dc=univ-paris1,dc=fr': {
                'employeeType': ['Professeur des universités', 'Directeur de recherche'],
                'supannEtablissement': ['{UAI}0753364Z', '{UAI}0258465Z'],
            },
        },
        "local_value_position_dict": {"Professeur des universités":
                                          ("PR", "Professeur"),
                                      "Directeur de recherche":
                                          ("DR", "Directeur de recherche et assimilés")
                                      },
    }
], indirect=True)
def test_case_with_multiple_affectations_in_different_entities(dag, unique_logical_date):
    """
    Test that the employment are converted from the LDAP entry when multiple employeeType and
    supannEtablissement are known
    :param dag: The DAG object
    :param unique_logical_date: unique execution date
    :return: None
    """
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_logical_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids=TEST_TASK_ID) == {
        'uid=hdupont,ou=people,dc=univ-paris1,dc=fr': {
            "employments": [
                {
                    "position": {
                        "title": "Professeur",
                        "code": "PR"
                    },
                    "main_research_structure": "UAI-0753364Z",
                },
                {
                    "position": {
                        "title": "Directeur de recherche et assimilés",
                        "code": "DR"
                    },
                    "main_research_structure": "UAI-0258465Z",
                }
            ]
        }
    }


@pytest.mark.parametrize("dag", [
    {
        "task_name": TESTED_TASK_NAME,
        "param_names": ["raw_results", "local_value_position_dict"],
        "raw_results": {
            'uid=hdupont,ou=people,dc=univ-paris1,dc=fr': {
                'employeeType': [],
                'supannEtablissement': [],
            },
        },
        "local_value_position_dict": {},
    }
], indirect=True)
def test_case_with_empty_informations(dag, unique_logical_date):
    """
    Test that the employment are returned empty when no datas are known
    :param dag: The DAG object
    :param unique_logical_date: unique execution date
    :return: None
    """
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_logical_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids=TEST_TASK_ID) == {
        'uid=hdupont,ou=people,dc=univ-paris1,dc=fr': {
            "employments": []
        }
    }


@pytest.mark.parametrize("dag", [
    {
        "task_name": TESTED_TASK_NAME,
        "param_names": ["raw_results", "local_value_position_dict"],
        "raw_results": {
            'uid=hdupont,ou=people,dc=univ-paris1,dc=fr': {
                'employeeType': ['Professeur des universités'],
                'supannEtablissement': ['{UAI}0753364Z', '{UAI}0258465Z'],
            },
        },
        "local_value_position_dict": {"Professeur des universités": ("PR", "Professeur")},
    }
], indirect=True)
def test_case_with_two_different_entities_and_one_known_affectation(dag, unique_logical_date):
    """
    Test that the employment are converted from the LDAP entry when one employeeType but two
    supannEtablissement are known
    :param dag: The DAG object
    :param unique_logical_date: unique execution date
    :return: None
    """
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_logical_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids=TEST_TASK_ID) == {
        'uid=hdupont,ou=people,dc=univ-paris1,dc=fr': {
            "employments": [
                {
                    "position": {
                        "title": "Professeur",
                        "code": "PR"
                    },
                    "main_research_structure": "UAI-0753364Z",
                },
                {
                    "position": {},
                    "main_research_structure": "UAI-0258465Z",
                }
            ]
        }
    }


@pytest.mark.parametrize("dag", [
    {
        "task_name": TESTED_TASK_NAME,
        "param_names": ["raw_results", "local_value_position_dict"],
        "raw_results": {
            'uid=hdupont,ou=people,dc=univ-paris1,dc=fr': {
                'employeeType': [
                    "Chargé d'enseignement",
                    'Maître de conférences'
                ],
                'supannEtablissement': [
                    '{UAI}0753364Z'
                ],
            },
        },
        "local_value_position_dict": {"Maître de conférences": ("MCF", "Maître de conférences"),
                                      "Chargé d'enseignement": ("PCAP", "Professeur certifié")
                                      },
    }
], indirect=True)
def test_case_with_one_entity_and_two_known_affectation(dag, unique_logical_date):
    """
    Test that the employment are converted from the LDAP entry when 2 employeeType are known for 1
    supannEtablissement
    :param dag: The DAG object
    :param unique_logical_date: unique execution date
    :return: None
    """
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_logical_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids=TEST_TASK_ID) == {
        'uid=hdupont,ou=people,dc=univ-paris1,dc=fr': {
            "employments": [
                {
                    "position": {
                        "title": "Professeur certifié",
                        "code": "PCAP"
                    },
                    "main_research_structure": "UAI-0753364Z",
                },
                {
                    "position": {
                        "title": "Maître de conférences",
                        "code": "MCF"
                    },
                    "main_research_structure": "UAI-0753364Z",
                }
            ]
        }
    }


@pytest.mark.parametrize("dag", [
    {
        "task_name": TESTED_TASK_NAME,
        "param_names": ["raw_results", "local_value_position_dict"],
        "raw_results": {
            'uid=hdupont,ou=people,dc=univ-paris1,dc=fr': {
                'employeeType': [
                    "Chargé d'enseignement",
                    'Maître de conférences',
                    "Conservateur general des bibliothèques"
                ],
                'supannEtablissement': [
                    '{UAI}0753364Z',
                    '{UAI}0258465Z'
                ],
            },
        },
        "local_value_position_dict": {},
    }
], indirect=True)
def test_case_with_multiples_entities_and_more_known_affectation(dag, unique_logical_date):
    """
    Test that the employment are converted from the LDAP entry when 2 employeeType are known for 1
    supannEtablissement
    :param dag: The DAG object
    :param unique_logical_date: unique execution date
    :return: None
    """
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_logical_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids=TEST_TASK_ID) == {
        'uid=hdupont,ou=people,dc=univ-paris1,dc=fr': {
            "employments": [
                {
                    "position": {},
                    "main_research_structure": "UAI-0753364Z",
                },
                {
                    "position": {},
                    "main_research_structure": "UAI-0258465Z",
                }
            ]
        }
    }


@pytest.mark.parametrize("dag", [
    {
        "task_name": TESTED_TASK_NAME,
        "param_names": ["raw_results", "local_value_position_dict"],
        "raw_results": {
            'uid=jdubois,ou=people,dc=univ-paris1,dc=fr': {
                'employeeType': ['Professeur Intergalactique'],
                'supannEtablissement': ['{UAI}0753364Z'],
            },
        },
        "local_value_position_dict": {},
    }
], indirect=True)
def test_unknown_employee_type_logs_warning(dag, unique_logical_date, caplog):
    """
    Test that when an employeeType is not found in YAML.local_values,
    the position is returned as {} and a warning is logged.
    """
    caplog.set_level(logging.WARNING)

    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_logical_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)

    assert ti.state == TaskInstanceState.SUCCESS

    result = ti.xcom_pull(task_ids=TEST_TASK_ID)
    assert result == {
        'uid=jdubois,ou=people,dc=univ-paris1,dc=fr': {
            "employments": [
                {
                    "position": {},
                    "main_research_structure": "UAI-0753364Z",
                }
            ]
        }
    }

    warnings = [rec.message for rec in caplog.records if rec.levelno == logging.WARNING]
    assert any("not found in YAML local_values" in msg for msg in warnings)

    @pytest.mark.parametrize("dag", [
        {
            "task_name": TESTED_TASK_NAME,
            "param_names": ["raw_results", "local_value_position_dict"],
            "raw_results": {
                'uid=jnone,ou=people,dc=univ-paris1,dc=fr': {
                    'employeeType': [None],
                    'supannEtablissement': ['{UAI}0753364Z'],
                },
            },
            "local_value_position_dict": {},
        }
    ], indirect=True)
    def test_employee_type_none_returns_empty_position(dag, unique_logical_date):
        """
        Test that when LDAP returns employeeType = [None],
        the code does not crash and returns an empty position for the establishment.
        """
        dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_logical_date)
        ti = create_task_instance(dag, dag_run, TEST_TASK_ID)

        ti.run(ignore_ti_state=True)
        assert ti.state == TaskInstanceState.SUCCESS

        result = ti.xcom_pull(task_ids=TEST_TASK_ID)
        assert result == {
            'uid=jnone,ou=people,dc=univ-paris1,dc=fr': {
                "employments": [
                    {
                        "position": {},
                        "main_research_structure": "UAI-0753364Z",
                    }
                ]
            }
        }
