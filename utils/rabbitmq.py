import logging

from airflow.exceptions import AirflowNotFoundException
from airflow.hooks.base import BaseHook
from airflow.models import Connection
from airflow.utils.db import provide_session
from rabbitmq_provider.hooks.rabbitmq import RabbitMQHook

from utils.config import get_env_variable

logger = logging.getLogger(__name__)


def get_rabbitmq_hook() -> RabbitMQHook | None:
    """
    Get the RabbitMQ hook from the Airflow managed connections.
    :return: The RabbitMQ hook or None if it does not exist
    """
    rabbitmq_conn_id = get_rabbitmq_conn_id()
    print(f"RabbitMQ connection ID: {rabbitmq_conn_id}")
    try:
        # looking for the hook is not enough,
        # it would be non null even if the connection does not exist
        conn = BaseHook.get_connection(rabbitmq_conn_id)
        logger.info("RabbitMQ connection: %s", conn)
        hook = RabbitMQHook(rabbitmq_conn_id=rabbitmq_conn_id)
        logger.info("RabbitMQ hook: %s", hook)
    except AirflowNotFoundException as e:
        logger.warning("No existing connection found: %s", str(e))
        return None
    return hook


def get_rabbitmq_conn_id() -> str:
    """
    Get the RabbitMQ connection ID from the environment variables.
    :return: The RabbitMQ connection ID
    """
    rabbitmq_conn_id = get_env_variable("RABBITMQ_CONN_ID")
    assert rabbitmq_conn_id is not None, "No Rabbitmq connection ID found"
    return rabbitmq_conn_id


@provide_session
def create_rabbitmq_managed_connection(session=None):
    """
    Create an Airflow managed RabbitMQ connection.
    :param session: The SQLAlchemy session
    :return:
    """
    connection = Connection(
        conn_id=get_rabbitmq_conn_id(),
        conn_type='rabbitmq',
        host=get_env_variable("AMQP_HOST"),
        login=get_env_variable("AMQP_USER"),
        password=get_env_variable("AMQP_PASSWORD"),
        port=get_env_variable("AMQP_PORT"),
    )
    logger.info(
        "Creating RabbitMQ connection: %s with connection id <%s>",
        connection,
        connection.conn_id
    )
    session.add(connection)
    session.commit()
