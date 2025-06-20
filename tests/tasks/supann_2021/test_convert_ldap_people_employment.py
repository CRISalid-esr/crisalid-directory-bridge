# pylint: disable=duplicate-code
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
        "param_names": ["raw_results"],
        "raw_results": {
            'uid=hdupont,ou=people,dc=univ-paris1,dc=fr': {
                'employeeType': ['Professeur des universités'],
                'supannEtablissement': ['{UAI}0753364Z'],
            },
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
                    "entity_uid": "UAI-0753364Z",
                }
            ]
        }
    }


@pytest.mark.parametrize("dag", [
    {
        "task_name": TESTED_TASK_NAME,
        "param_names": ["raw_results"],
        "raw_results": {
            'uid=hdupont,ou=people,dc=univ-paris1,dc=fr': {
                'employeeType': ['Professeur des universités', 'Directeur de recherche'],
                'supannEtablissement': ['{UAI}0753364Z', '{UAI}0258465Z'],
            },
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
                    "entity_uid": "UAI-0753364Z",
                },
                {
                    "position": {
                        "title": "Directeur de recherche et assimilés",
                        "code": "DR"
                    },
                    "entity_uid": "UAI-0258465Z",
                }
            ]
        }
    }


@pytest.mark.parametrize("dag", [
    {
        "task_name": TESTED_TASK_NAME,
        "param_names": ["raw_results"],
        "raw_results": {
            'uid=hdupont,ou=people,dc=univ-paris1,dc=fr': {
                'employeeType': [],
                'supannEtablissement': [],
            },
        },
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
        "param_names": ["raw_results"],
        "raw_results": {
            'uid=hdupont,ou=people,dc=univ-paris1,dc=fr': {
                'employeeType': ['Professeur des universités'],
                'supannEtablissement': ['{UAI}0753364Z', '{UAI}0258465Z'],
            },
        },
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
                    "entity_uid": "UAI-0753364Z",
                },
                {
                    "position": {},
                    "entity_uid": "UAI-0258465Z",
                }
            ]
        }
    }


@pytest.mark.parametrize("dag", [
    {
        "task_name": TESTED_TASK_NAME,
        "param_names": ["raw_results"],
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
                    "entity_uid": "UAI-0753364Z",
                },
                {
                    "position": {
                        "title": "Maître de conférences",
                        "code": "MCF"
                    },
                    "entity_uid": "UAI-0753364Z",
                }
            ]
        }
    }


@pytest.mark.parametrize("dag", [
    {
        "task_name": TESTED_TASK_NAME,
        "param_names": ["raw_results"],
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
                    "entity_uid": "UAI-0753364Z",
                },
                {
                    "position": {},
                    "entity_uid": "UAI-0258465Z",
                }
            ]
        }
    }
