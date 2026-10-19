import datetime
from typing import Any

import pendulum
from airflow import DAG
from airflow.models import DagRun, DagModel
from airflow.models import TaskInstance
from airflow.models.dag_version import DagVersion
from airflow.models.dagbundle import DagBundleModel
from airflow.models.serialized_dag import SerializedDagModel
from airflow.serialization.serialized_objects import LazyDeserializedDAG
from airflow.timetables.base import DataInterval
from airflow.utils.session import provide_session
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType, DagRunTriggeredByType
from sqlalchemy import select
from sqlalchemy.orm import Session

BUNDLE_NAME = "tests"
TEST_DAG_ID = "fixture_dag_id"
DATA_INTERVAL_START = pendulum.datetime(2024, 1, 1, tz="UTC")
DATA_INTERVAL_END = DATA_INTERVAL_START + datetime.timedelta(days=1)


def assert_dag_dict_equal(structure: dict, dag: DAG) -> None:
    """
    Controls the internal structure of the DAG by comparing the task_dict
    to a provided dictionary
    :param structure: Dictionary containing the structure of the DAG
    :param dag: DAG object to compare
    :return: None
    """
    assert dag.task_dict.keys() == structure.keys()
    for task_id, downstream_list in structure.items():
        assert dag.has_task(task_id)
        task = dag.get_task(task_id)
        assert task.downstream_task_ids == set(downstream_list)


@provide_session
def create_dag_run(
        dag,
        data_interval_start,
        data_interval_end,
        logical_date,
        *,
        run_id: str | None = None,
        conf: Any | None = None,
        state: DagRunState = DagRunState.RUNNING,
        session: Session,
) -> DagRun:
    if run_id is None:
        run_id = f"manual__{logical_date.isoformat()}"

    # Ensure DagBundle exists
    bundle = session.scalar(select(DagBundleModel).where(DagBundleModel.name == BUNDLE_NAME))
    if bundle is None:
        bundle = DagBundleModel(
            name=BUNDLE_NAME,
        )
        session.add(bundle)
        session.flush()

    # Ensure DagModel exists
    dm = session.scalar(select(DagModel).where(DagModel.dag_id == dag.dag_id))
    if dm is None:
        dm = DagModel(dag_id=dag.dag_id, bundle_name=BUNDLE_NAME)
        dm.is_active = True
        dm.is_paused = False
        session.add(dm)
        session.flush()
    elif dm.bundle_name != BUNDLE_NAME:
        dm.bundle_name = BUNDLE_NAME
        session.flush()

    # Serialize DAG to DB
    lazy = LazyDeserializedDAG.from_dag(dag)
    SerializedDagModel.write_dag(dag=lazy, bundle_name=BUNDLE_NAME, session=session)
    session.flush()

    # Get latest dag version
    dag_version = DagVersion.get_latest_version(dag.dag_id, session=session)
    if not dag_version:
        raise RuntimeError(f"No DagVersion found for {dag.dag_id} after serialization")

    # Create DagRun
    dr = DagRun(
        dag_id=dag.dag_id,
        run_id=run_id,
        run_type=DagRunType.MANUAL,
        triggered_by=DagRunTriggeredByType.TEST,
        logical_date=logical_date,
        data_interval=DataInterval(start=data_interval_start, end=data_interval_end),
        start_date=data_interval_end,
        run_after=pendulum.now("UTC"),
        state=state,
        conf=conf or {},
        bundle_version=dag_version.bundle_version,
    )
    session.add(dr)
    session.flush()
    return dr


@provide_session
def create_task_instance(
        dag: DAG,
        dag_run: DagRun,
        task_id: str,
        *,
        session: Session,
) -> TaskInstance:
    """
    Create (and persist) a TaskInstance for task_id for the given dag_run.
    """

    task = dag.get_task(task_id=task_id)
    serialized = SerializedDagModel.get(dag.dag_id)
    if not serialized:
        raise RuntimeError(f"No SerializedDagModel found for {dag.dag_id}")

    ti = TaskInstance(
        task=task,
        run_id=dag_run.run_id,
        dag_version_id=serialized.dag_version_id
    )

    # Persist so ti.run() can find it cleanly
    session.add(ti)
    session.flush()

    return ti
