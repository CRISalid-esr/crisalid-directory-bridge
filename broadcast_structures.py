import logging

import pendulum
from airflow.decorators import dag, task
from airflow.sensors.external_task import ExternalTaskSensor

from tasks.broadcast.send_status_messages import send_status_messages
from tasks.compute_status import compute_structure_status
from tasks.database import read_keys_from_redis, create_redis_connection_task, \
    read_structure_with_scores_from_redis
from tasks.rabbitmq import create_rabbitmq_connection_task

logger = logging.getLogger(__name__)


@dag(
    dag_id="broadcast_structures",
    start_date=pendulum.datetime(2024, 7, 2, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["structures", "amqp"],
    max_active_tasks=10,
    default_args={
        "depends_on_past": False,
    }
)
def broadcast_structures():
    """
    This DAG fetches data from an LDAP server, processes specific fields in parallel,
    and then combines the results into a target JSON structure.
    """

    wait_for_ldap_structures = ExternalTaskSensor(
        task_id='wait_for_ldap_structures',
        external_dag_id='load_ldap_structures',
        external_task_id='trigger_broadcast',
        execution_date_fn=lambda dt: dt,  # Use the same execution date
    )

    @task
    def print_execution_date(**kwargs):
        timestamp = kwargs['dag_run'].conf.get('timestamp')
        print(f"Execution date as Unix timestamp: {timestamp}")
        return {"date": timestamp}

    execution_date = print_execution_date()

    wait_for_ldap_structures >> execution_date  # pylint: disable=pointless-statement

    redis_connection = create_redis_connection_task()

    keys = read_keys_from_redis(prefix="struct:ldap:")

    structure_data_with_scores = read_structure_with_scores_from_redis.expand(redis_key=keys)

    structure_data_with_statuses = compute_structure_status.expand(
        structure_data_with_scores=structure_data_with_scores)

    send_status_messages.expand(
        structure_data_with_status=structure_data_with_statuses)

    rabbitmq_connection = create_rabbitmq_connection_task()

    # pylint: disable=pointless-statement
    execution_date \
    >> redis_connection \
    >> rabbitmq_connection \
    >> structure_data_with_scores


broadcast_structures()
