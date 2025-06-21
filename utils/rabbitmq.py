# file: utils/rabbitmq.py
import logging

from airflow.exceptions import AirflowNotFoundException
from airflow.hooks.base import BaseHook
from rabbitmq_provider.hooks.rabbitmq import RabbitMQHook

from utils.config import get_env_variable

logger = logging.getLogger(__name__)


def get_rabbitmq_hook() -> RabbitMQHook | None:
    """
    Get the RabbitMQ hook from the Airflow managed connections.
    :return: The RabbitMQ hook or None if it does not exist
    """
    rabbitmq_conn_id = get_rabbitmq_conn_id()
    try:
        conn = BaseHook.get_connection(rabbitmq_conn_id)
        logger.info("RabbitMQ connection: %s", conn)
        return RabbitMQHook(rabbitmq_conn_id=rabbitmq_conn_id)
    except AirflowNotFoundException as e:
        logger.warning("No existing RabbitMQ connection found: %s", str(e))
        return None


def get_rabbitmq_conn_id() -> str:
    """
    Get the RabbitMQ connection ID from the environment variables.
    :return: The RabbitMQ connection ID
    """
    rabbitmq_conn_id = get_env_variable("RABBITMQ_CONN_ID")
    assert rabbitmq_conn_id is not None, "No RabbitMQ connection ID found"
    return rabbitmq_conn_id
