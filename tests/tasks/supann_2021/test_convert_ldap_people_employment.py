# pylint: disable=duplicate-code
import pytest
from airflow.utils.state import TaskInstanceState

from test_utils.dags import create_dag_run, \
    create_task_instance, \
    DATA_INTERVAL_START, \
    DATA_INTERVAL_END

TEST_TASK_ID = "convert_ldap_people_employment"

TESTED_TASK_NAME = "tasks.supann_2021.convert_ldap_people_employment.convert_ldap_people_employment"

BASE_FOR_TESTS = {
    'uid=hdupont,ou=people,dc=univ-paris1,dc=fr': {
        'uid': ['soussiat'],
        'sn': ['Soussiat'],
        'givenName': ['Sylvain'],
        'cn': ['Soussiat Sylvain'],
        'displayName': ['Sylvain Soussiat'],
        'supannRefId': ['{MIFARE}803853BA631B04', '{SIHAM}UP1000010877',
                        '{HARPEGE}10877', '{APOGEE.PER}15817',
                        '{BARCODE.HISTORY}201000004689',
                        '{MIFARE.HISTORY}803853BA631B04', '{SEAL}51027',
                        '{SEAL:ID}51027'],
        'eduPersonOrgDN': ['o=Paris1,dc=univ-paris1,dc=fr'],
        'supannEntiteAffectation': ['U02'],
        'employeeType': ['Professeur des universités'],
        'eduPersonAffiliation': ['member', 'teacher', 'faculty', 'researcher', 'employee'],
        'supannEntiteAffectationPrincipale': ['U02'],
        'supannEtablissement': ['{UAI}0753364Z'],
        'eduPersonPrimaryOrgUnitDN': ['supannCodeEntite=U02,'
                                      'ou=structures,dc=univ-paris1,dc=fr'],
        'eduPersonOrgUnitDN': ['supannCodeEntite=U02,ou=structures,dc=univ-paris1,dc=fr'],
        'eduPersonPrimaryAffiliation': ['faculty'],
        'postalAddress': ['8 bis rue de la Croix Jarry$75644 PARIS cedex 13$France'],
        'mail': ['sylvain.soussiat@univ-paris1.fr'],
        'labeledURI': ['http://perso.univ-paris1.fr/soussiat']
    },
}


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
def test_usual_case(dag, unique_execution_date):
    """
    Test that the employment are converted from the LDAP entry when only employeeType and
    supannEtablissement are known
    :param dag: The DAG object
    :param unique_execution_date: unique execution date
    :return: None
    """
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_execution_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids=TEST_TASK_ID) == {
        'uid=hdupont,ou=people,dc=univ-paris1,dc=fr': {
            "employments": [
                {
                    "position": {
                        "title": "Professeur des universités",
                        "code": "pr"
                    },
                    "entity_uid": "uai-0753364Z"
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
def test_case_with_multiple_affectations_in_different_entities(dag, unique_execution_date):
    """
    Test that the employment are converted from the LDAP entry when multiple employeeType and
    supannEtablissement are known
    :param dag: The DAG object
    :param unique_execution_date: unique execution date
    :return: None
    """
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_execution_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids=TEST_TASK_ID) == {
        'uid=hdupont,ou=people,dc=univ-paris1,dc=fr': {
            "employments": [
                {
                    "position": {
                        "title": "Professeur des universités",
                        "code": "pr"
                    },
                    "entity_uid": "uai-0753364Z"
                },
                {
                    "position": {
                        "title": "Directeur de recherche",
                        "code": "dr"
                    },
                    "entity_uid": "uai-0258465Z"
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
def test_case_with_empty_informations(dag, unique_execution_date):
    """
    Test that the employment are returned empty when no datas are known
    :param dag: The DAG object
    :param unique_execution_date: unique execution date
    :return: None
    """
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_execution_date)
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
def test_case_with_two_different_entities_and_one_known_affectation(dag, unique_execution_date):
    """
    Test that the employment are converted from the LDAP entry when one employeeType but two
    supannEtablissement are known
    :param dag: The DAG object
    :param unique_execution_date: unique execution date
    :return: None
    """
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_execution_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids=TEST_TASK_ID) == {
        'uid=hdupont,ou=people,dc=univ-paris1,dc=fr': {
            "employments": [
                {
                    "position": {
                        "title": "Professeur des universités",
                        "code": "pr"
                    },
                    "entity_uid": "uai-0753364Z"
                },
                {
                    "position": {},
                    "entity_uid": "uai-0258465Z"
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
def test_case_with_one_entity_and_two_known_affectation(dag, unique_execution_date):
    """
    Test that the employment are converted from the LDAP entry when 2 employeeType are known for 1
    supannEtablissement
    :param dag: The DAG object
    :param unique_execution_date: unique execution date
    :return: None
    """
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_execution_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids=TEST_TASK_ID) == {
        'uid=hdupont,ou=people,dc=univ-paris1,dc=fr': {
            "employments": [
                {
                    "position": {
                        "title": "Chargé d'enseignement",
                        "code": "cce"
                    },
                    "entity_uid": "uai-0753364Z"
                },
                {
                    "position": {
                        "title": "Maître de conférences",
                        "code": "mcf"
                    },
                    "entity_uid": "uai-0753364Z"
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
def test_case_with_multiples_entities_and_more_known_affectation(dag, unique_execution_date):
    """
    Test that the employment are converted from the LDAP entry when 2 employeeType are known for 1
    supannEtablissement
    :param dag: The DAG object
    :param unique_execution_date: unique execution date
    :return: None
    """
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_execution_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids=TEST_TASK_ID) == {
        'uid=hdupont,ou=people,dc=univ-paris1,dc=fr': {
            "employments": [
                {
                    "position": {},
                    "entity_uid": "uai-0753364Z"
                },
                {
                    "position": {},
                    "entity_uid": "uai-0258465Z"
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
                'supannEmpProfil': [
                    '[etab={UAI}0753364Z][affil=faculty][corps={NCORPS}300][typeaffect={SUPANN}S108][affect=COV1][population={SUPANN}RGPF]'
                ]
            },
        },
    }
], indirect=True)
def test_case_with_supannempprofil(dag, unique_execution_date):
    """
    Test that the employment are converted from the LDAP entry when a supannEmpProfil is available.
    :param dag: The DAG object
    :param unique_execution_date: unique execution date
    :return: None
    """
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_execution_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids=TEST_TASK_ID) == {
        'uid=hdupont,ou=people,dc=univ-paris1,dc=fr': {
            "employments": [
                {
                    "position": {
                        "title": "Professeur des universités",
                        "code": "pr"
                    },
                    "entity_uid": "uai-0753364Z"
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
                'supannEmpProfil': [
                    '[etab={UAI}0753364Z][affil=teacher][typeaffect={SUPANN}S108][affect=COV1][population={SUPANN}RGIE]',
                    '[etab={UAI}0753364Z][affil=faculty][corps={NCORPS}300][typeaffect={SUPANN}S108][affect=COV1][population={SUPANN}RGPF]'
                ]
            },
        },
    }
], indirect=True)
def test_case_with_two_supannempprofil(dag, unique_execution_date):
    """
    Test that the employment are converted from the LDAP entry when 2 supannEmpProfil are available.
    :param dag: The DAG object
    :param unique_execution_date: unique execution date
    :return: None
    """
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_execution_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids=TEST_TASK_ID) == {
        'uid=hdupont,ou=people,dc=univ-paris1,dc=fr': {
            "employments": [
                {
                    "position": {},
                    "entity_uid": "uai-0753364Z"
                },
                {
                    "position": {
                        "title": "Professeur des universités",
                        "code": "pr"
                    },
                    "entity_uid": "uai-0753364Z"
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
                'supannEmpProfil': [
                    '[etab={OTHER}0753364Z][affil=faculty][corps={NCORPS}300][typeaffect={'
                    'SUPANN}S108][affect=COV1][population={SUPANN}RGPF]'
                ]
            },
        },
    }
], indirect=True)
def test_case_with_supannempprofil_unexpected_etab_value(dag, unique_execution_date):
    """
    Test that the employment are converted from the LDAP entry when a supannEmpProfil is available.
    :param dag: The DAG object
    :param unique_execution_date: unique execution date
    :return: None
    """
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_execution_date)
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
                'supannEmpProfil': [
                    '[etab={UAI}0753364Z][affil=faculty][corps={OTHER}300][typeaffect={'
                    'SUPANN}S108][affect=COV1][population={SUPANN}RGPF]'
                ]
            },
        },
    }
], indirect=True)
def test_case_with_supannempprofil_unexpected_corps_value(dag, unique_execution_date):
    """
    Test that the employment are converted from the LDAP entry when a supannEmpProfil is available.
    :param dag: The DAG object
    :param unique_execution_date: unique execution date
    :return: None
    """
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_execution_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids=TEST_TASK_ID) == {
        'uid=hdupont,ou=people,dc=univ-paris1,dc=fr': {
            "employments": [
                {
                    "position": {},
                    "entity_uid": "uai-0753364Z"
                }
            ]
        }
    }
