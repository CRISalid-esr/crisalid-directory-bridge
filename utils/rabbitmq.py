import logging

from airflow.hooks.base import BaseHook
from pika import BlockingConnection, URLParameters
from pika.exchange_type import ExchangeType

from utils.config import get_env_variable

logger = logging.getLogger(__name__)


def get_rabbitmq_conn_id() -> str:
    """Get the RabbitMQ connection ID from the environment variables."""
    cid = get_env_variable("RABBITMQ_CONN_ID")
    assert cid, "No Rabbitmq connection ID found"
    return cid


def open_pika_channel():
    """Return (connection, channel) built from the Airflow Connection."""
    conn_id = get_rabbitmq_conn_id()
    conn = BaseHook.get_connection(conn_id)
    vhost = conn.schema or ""
    amqp_url = f"amqp://{conn.login}:{conn.password}@{conn.host}:{conn.port or 5672}/{vhost}"
    logger.info("Connecting to RabbitMQ: %s", amqp_url)
    connection = BlockingConnection(URLParameters(amqp_url))
    channel = connection.channel()
    # Ensure the exchange exists
    channel.exchange_declare(exchange="directory",
                             exchange_type=ExchangeType.topic,
                             durable=True)
    return connection, channel
