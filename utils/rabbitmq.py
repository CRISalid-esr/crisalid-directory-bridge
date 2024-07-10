import logging

from airflow.exceptions import AirflowNotFoundException
from airflow.models import Connection
from airflow.utils.db import provide_session
from rabbitmq_provider.hooks.rabbitmq import RabbitMQHook

from utils.config import get_env_variable

logger = logging.getLogger(__name__)


def get_rabbitmq_hook() -> RabbitMQHook | None:
    """
    Get the RabbitMQ hook from the Airflow RabbitMQ connection.
    :return: The RabbitMQ hook
    """
    rabbitmq_conn_id = get_rabbitmq_conn_id()
    try:
        hook = RabbitMQHook(rabbitmq_conn_id=rabbitmq_conn_id)
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
def create_rabbitmq_connection(session=None):
    """
    Create an Airflow managed RabbitMQ connection.
    :param session: The SQLAlchemy session
    :return:
    """
    connection = Connection(
        conn_id=get_rabbitmq_conn_id(),
        conn_type='rabbitmq',
        host=get_env_variable("RABBITMQ_HOST"),
        login=get_env_variable("RABBITMQ_USER"),
        password=get_env_variable("RABBITMQ_PASSWORD"),
        port=get_env_variable("RABBITMQ_PORT"),
    )
    session.add(connection)
    session.commit()
