import pytest
from airflow.utils.state import TaskInstanceState

from test_utils.dags import create_dag_run, \
    DATA_INTERVAL_START, \
    DATA_INTERVAL_END, \
    create_task_instance

TESTED_TASK_NAME = ("tasks.supann_2021.convert_ldap_structure_short_labels"
                    ".convert_ldap_structure_short_labels")
TEST_TASK_ID = "convert_ldap_structure_short_labels"


@pytest.mark.parametrize("dag", [
    {
        'task_name': TESTED_TASK_NAME,
        "param_names": ["raw_results"],
        'raw_results': {
            'U082': {
                'description': ['UEX\xa0: Laboratoire des Tests (UMR 2024)'],
                'ou': ['UMR 2024 - UEX'],
            },
        },
    },
], indirect=True)
def test_ou_field_is_used_as_short_label(dag, unique_logical_date) -> None:
    """Test that the ou field is used as the short label (takes priority over description regex)."""
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_logical_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids=TEST_TASK_ID) == {
        "U082": {'short_labels': [{'value': 'UMR 2024 - UEX', 'language': 'fr'}]}
    }


@pytest.mark.parametrize("dag", [
    {
        'task_name': TESTED_TASK_NAME,
        "param_names": ["raw_results"],
        'raw_results': {
            'U082': {
                'description': ['UEX\xa0: Laboratoire des Tests (UMR 2024)'],
            },
        },
    },
], indirect=True)
def test_description_acronym_used_as_fallback(dag, unique_logical_date) -> None:
    """Test that the acronym extracted from description is used when ou is absent."""
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_logical_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids=TEST_TASK_ID) == {
        "U082": {'short_labels': [{'value': 'UEX', 'language': 'fr'}]}
    }


@pytest.mark.parametrize("dag", [
    {
        'task_name': TESTED_TASK_NAME,
        "param_names": ["raw_results"],
        'raw_results': {
            "uid=1234,ou=people,dc=example,dc=org": {
                "eduorglegalname": ["University of Example"],
                "description": ["A university in Example"],
            },
        },
    },
], indirect=True)
def test_short_label_is_empty_if_not_extractable(dag, unique_logical_date) -> None:
    """Test that short_labels is empty when neither ou nor a parseable acronym is present."""
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_logical_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids=TEST_TASK_ID) == {
        "uid=1234,ou=people,dc=example,dc=org": {'short_labels': []}
    }
