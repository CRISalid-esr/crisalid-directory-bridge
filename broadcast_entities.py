import logging

import pendulum
from airflow.decorators import dag, task

from tasks.broadcast.send_status_messages import send_status_messages
from tasks.compute_status import compute_entity_statuses
from tasks.database import read_entity_keys_from_redis, create_redis_connection_task, \
    read_entities_with_scores_from_redis
from tasks.rabbitmq import create_rabbitmq_connection_task

logger = logging.getLogger(__name__)


@dag(
    dag_id="broadcast_entities",
    start_date=pendulum.datetime(2024, 7, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["structures", "people", "amqp"],
    max_active_tasks=10,
    default_args={
        "depends_on_past": False,
    }
)
def broadcast_entities():
    """
    This DAG reads entity states from Redis, computes events statuses,
    and broadcasts them to RabbitMQ.
    """

    @task
    def read_timestamp(**kwargs):
        return kwargs['dag_run'].conf.get('timestamp')

    @task
    def read_entity_type(**kwargs):
        return kwargs['dag_run'].conf.get('entity_type')

    @task
    def read_entity_source(**kwargs):
        return kwargs['dag_run'].conf.get('entity_source')

    timestamp = read_timestamp()
    entity_type = read_entity_type()
    entity_source = read_entity_source()

    redis_connection = create_redis_connection_task()

    entity_keys = read_entity_keys_from_redis(entity_type=entity_type, entity_source=entity_source)

    entities_with_scores = read_entities_with_scores_from_redis(entity_keys=entity_keys,
                                                                timestamp=timestamp)

    entities_with_statuses = compute_entity_statuses(
        entities_with_scores=entities_with_scores, timestamp=timestamp)

    status_messages = send_status_messages(
        entities_with_statuses=entities_with_statuses, entity_type=entity_type)

    rabbitmq_connection = create_rabbitmq_connection_task()

    rabbitmq_connection >> status_messages  # pylint: disable=pointless-statement

    # pylint: disable=pointless-statement
    redis_connection >> entity_keys >> entities_with_scores >> entities_with_statuses


broadcast_entities()
