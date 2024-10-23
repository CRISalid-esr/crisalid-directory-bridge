# pylint: disable=duplicate-code
import pytest
from airflow.utils.state import TaskInstanceState

from test_utils.dags import create_dag_run, \
    create_task_instance, \
    DATA_INTERVAL_START, \
    DATA_INTERVAL_END

TEST_TASK_ID = "convert_ldap_people_memberships"

TESTED_TASK_NAME = 'tasks.supann_2021.convert_ldap_people_memberships' \
                   '.convert_ldap_people_memberships'


@pytest.mark.parametrize("dag", [
    {
        "task_name": TESTED_TASK_NAME,
        "param_names": ["raw_results"],
        "raw_results": {
            "uid=1234,ou=people,dc=example,dc=org": {
                "supannEntiteAffectationPrincipale": ["mainEntity"],
                "supannEntiteAffectation": ["entity1", "entity2"],
            },
        },
    }
], indirect=True)
def test_memberships_are_converted_from_ldap(dag, unique_execution_date):
    """
    Test that the memberships are converted from LDAP data
    :param dag:
    :param unique_execution_date:
    :return:
    """
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_execution_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids=TEST_TASK_ID) == {
        "uid=1234,ou=people,dc=example,dc=org": {
            "memberships": [
                {"entity_uid": "mainEntity"},
                {"entity_uid": "entity1"},
                {"entity_uid": "entity2"}
            ]
        }
    }


@pytest.mark.parametrize("dag", [
    {
        "task_name": TESTED_TASK_NAME,
        "param_names": ["raw_results"],
        "raw_results": {
            "uid=1234,ou=people,dc=example,dc=org": {
                "supannEntiteAffectation": ["entity1", "entity2"],
            },
        },
    }
], indirect=True)
def test_only_supann_entite_affectation_present(dag, unique_execution_date):
    """
    Test that if only supannEntiteAffectation is present, it is converted correctly.
    :param dag: The DAG object
    :param unique_execution_date: unique execution date
    :return: None
    """
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_execution_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids=TEST_TASK_ID) == {
        "uid=1234,ou=people,dc=example,dc=org": {
            "memberships": [
                {"entity_uid": "entity1"},
                {"entity_uid": "entity2"}
            ]
        }
    }


@pytest.mark.parametrize("dag", [
    {
        "task_name": TESTED_TASK_NAME,
        "param_names": ["raw_results"],
        "raw_results": {
            "uid=1234,ou=people,dc=example,dc=org": {
                "supannEntiteAffectationPrincipale": ["mainEntity"],
            },
        },
    }
], indirect=True)
def test_only_supann_entite_main_affectation_present(dag, unique_execution_date):
    """
    Test that if only supannEntiteAffectationPrincipale is present, it is converted correctly.
    :param dag: The DAG object
    :param unique_execution_date: unique execution date
    :return: None
    """
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_execution_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids=TEST_TASK_ID) == {
        "uid=1234,ou=people,dc=example,dc=org": {
            "memberships": [
                {"entity_uid": "mainEntity"}
            ]
        }
    }


@pytest.mark.parametrize("dag", [
    {
        "task_name": TESTED_TASK_NAME,
        "param_names": ["raw_results"],
        "raw_results": {
            "uid=1234,ou=people,dc=example,dc=org": {},
        },
    }
], indirect=True)
def test_no_memberships_present(dag, unique_execution_date):
    """
    Test that if no memberships are present, the memberships field is empty.
    :param dag: The DAG object
    :param unique_execution_date: unique execution date
    :return: None
    """
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_execution_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids=TEST_TASK_ID) == {
        "uid=1234,ou=people,dc=example,dc=org": {
            "memberships": []
        }
    }


@pytest.mark.parametrize("dag", [
    {
        "task_name": TESTED_TASK_NAME,
        "param_names": ["raw_results"],
        "raw_results": {
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
        },
    }
], indirect=True)
def test_real_case(dag, unique_execution_date):
    """
    Test that if supannEntiteAffectationPrincipale is present, it is converted correctly.
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
            'memberships': [
                {'entity_uid': 'U02'}
            ]
        }
    }
