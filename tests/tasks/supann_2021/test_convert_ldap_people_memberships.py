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
        "ldap_results": {
            "uid=1234,ou=people,dc=example,dc=org": {
                "supannEntiteAffectationPrincipale": "mainEntity",
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
                {"entity": "mainEntity"},
                {"entity": "entity1"},
                {"entity": "entity2"}
            ]
        }
    }


@pytest.mark.parametrize("dag", [
    {
        "task_name": TESTED_TASK_NAME,
        "ldap_results": {
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
                {"entity": "entity1"},
                {"entity": "entity2"}
            ]
        }
    }


@pytest.mark.parametrize("dag", [
    {
        "task_name": TESTED_TASK_NAME,
        "ldap_results": {
            "uid=1234,ou=people,dc=example,dc=org": {
                "supannEntiteAffectationPrincipale": "mainEntity",
            },
        },
    }
], indirect=True)
def test_only_supann_entite_affectation_principale_present(dag, unique_execution_date):
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
                {"entity": "mainEntity"}
            ]
        }
    }


@pytest.mark.parametrize("dag", [
    {
        "task_name": TESTED_TASK_NAME,
        "ldap_results": {
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
        "ldap_results": {
            'uid=hdupont,ou=people,dc=univ-paris1,dc=fr': {
                'supannListeRouge': 'FALSE',
                'objectClass': 'person',
                'uid': 'hdupont',
                'supannAliasLogin': 'hdupont',
                'uidNumber': '123456',
                'gidNumber': '2000000',
                'gecos': 'Hélène Dupont',
                'displayName': 'Hélène Dupont',
                'cn': 'Dupont Hélène',
                'sn': 'Dupont',
                'givenName': 'Hélène',
                'supannCivilite': 'M.',
                'supannEntiteAffectation': 'U02',
                'supannOrganisme': '{EES}0751717J',
                'departmentNumber': 'CNU 05',
                'employeeType': 'Professeur des universités',
                'supannActivite': '{CNU}0500',
                'eduPersonAffiliation': 'member',
                'eduPersonPrimaryAffiliation': 'teacher',
                'postalAddress': '48 BOULEVARD JOURDAN$75014 PARIS$FRANCE',
                'supannEntiteAffectationPrincipale': 'U02',
                'supannEtablissement': '{UAI}0751717J',
                'buildingName': 'Campus Jourdan',
                'eduPersonOrgUnitDN': 'ou=U02,ou=structures,o=Paris1,dc=univ-paris1,dc=fr',
                'eduPersonPrimaryOrgUnitDN': 'ou=U02,ou=structures,o=Paris1,dc=univ-paris1,dc=fr',
                'eduPersonOrgDN': 'supannCodeEntite=UP1,ou=structures,dc=univ-paris1,dc=fr',
                'labeledURI': 'http://perso.univ-paris1.fr/hdupont'
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
                {'entity': 'U02'}
            ]
        }
    }
