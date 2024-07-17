import pytest
from airflow.utils.state import TaskInstanceState

from test_utils.dags import create_dag_run, \
    DATA_INTERVAL_START, \
    DATA_INTERVAL_END, \
    create_task_instance

TESTED_TASK_NAME = 'tasks.supann_2021.convert_ldap_structure_contacts' \
                   '.convert_ldap_structure_contacts'
TEST_TASK_ID = "convert_ldap_structure_contacts"


@pytest.mark.parametrize("dag", [
    {
        'task_name': TESTED_TASK_NAME,
        'ldap_results': {
            "uid=1234,ou=people,dc=example,dc=org": {
                "postalAddress": "Center Meudon$1 PLACE ARISTIDE BRIAND$92190 MEUDON$France",
                "eduorglegalname": "University of Example",
            },
            "uid=5678,ou=people,dc=example,dc=org": {
                "postalAddress": "Another Center$456 Another Street$12345 Another City",
                "eduorglegalname": "Another University",
            },
            "uid=91011,ou=people,dc=example,dc=org": {
                "postalAddress": "Simple Address Without Proper Format",
                "eduorglegalname": "Simple University",
            }
        }
    }
], indirect=True)
def test_contacts_are_converted_from_ldap(dag, unique_execution_date) -> None:
    """
    Test that the contacts are converted from the LDAP entry
    :param dag: The DAG object
    :param unique_execution_date: The unique execution date
    :return: None
    """
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_execution_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    expected_results = {
        "uid=1234,ou=people,dc=example,dc=org": {"contacts": [{
            'type': 'postal_address',
            'format': 'structured_physical_address',
            'value': {
                'street': 'Center Meudon, 1 PLACE ARISTIDE BRIAND',
                'city': 'MEUDON',
                'zip_code': '92190',
                'country': 'France'
            }
        }]},
        "uid=5678,ou=people,dc=example,dc=org": {"contacts": [{
            'type': 'postal_address',
            'format': 'structured_physical_address',
            'value': {
                'street': 'Another Center, 456 Another Street',
                'city': 'Another City',
                'zip_code': '12345'
            }
        }]},
        "uid=91011,ou=people,dc=example,dc=org": {"contacts": [{
            'type': 'postal_address',
            'format': 'simple_physical_address',
            'value': {'address': 'Simple Address Without Proper Format'}
        }]}
    }

    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids=TEST_TASK_ID) == expected_results
