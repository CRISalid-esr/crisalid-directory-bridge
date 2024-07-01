import datetime

import pendulum
import pytest
from _pytest.fixtures import SubRequest
from airflow import DAG
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType

from utils.dependencies import import_task

TEST_TASK_ID = "convert_ldap_structure_name_task"
TEST_DAG_ID = "fixture_dag_id"
DATA_INTERVAL_START = pendulum.datetime(2024, 1, 1, tz="UTC")
DATA_INTERVAL_END = DATA_INTERVAL_START + datetime.timedelta(days=1)


@pytest.fixture()
def dag(request: SubRequest) -> DAG:
    """
    Create a DAG for testing the convert_ldap_structure_name_task
    :param request: The pytest request object
    :return: The DAG object
    """
    with DAG(
            dag_id=TEST_DAG_ID,
            schedule="@daily",
            start_date=DATA_INTERVAL_START,
    ) as dag:
        convert_name_task = import_task(
            "tasks.supann_2021.convert_ldap_structure_name.convert_ldap_structure_name_task")
        ldap_results = request.param
        convert_name_task(ldap_results)
    return dag


@pytest.mark.parametrize("dag", [
    {
        "dn": "uid=1234,ou=people,dc=example,dc=org",
        "entry": {
            "eduorglegalname": "University of Example",
            "description": "A university in Example",
        },
    }
], indirect=True)
def test_name_is_converted_from_ldap(dag):
    dag_run = dag.create_dagrun(
        state=DagRunState.RUNNING,
        execution_date=DATA_INTERVAL_START,
        data_interval=(DATA_INTERVAL_START, DATA_INTERVAL_END),
        start_date=DATA_INTERVAL_END,
        run_type=DagRunType.MANUAL,
    )
    ti = dag_run.get_task_instance(task_id=TEST_TASK_ID)
    ti.task = dag.get_task(task_id=TEST_TASK_ID)

    ti.run(ignore_ti_state=True)
    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids=TEST_TASK_ID) == "University of Example"
