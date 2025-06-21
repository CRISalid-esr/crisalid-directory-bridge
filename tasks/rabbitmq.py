import logging

from airflow.decorators import task
from airflow.models import Connection

from utils.rabbitmq import get_rabbitmq_conn_id, get_rabbitmq_hook

logger = logging.getLogger(__name__)


@task
def get_rabbitmq_connection() -> Connection:
    """
    Get or create the Airflow managed RabbitMQ connection.
    :return: The RabbitMQ connection
    """
    connexion = get_rabbitmq_hook()
    if connexion is None:
        logger.error("No RabbitMQ connection found. "
                     "Check your AIRFLOW_CONN_CRISALID_BUS environment variable.")
        raise ConnectionError("No RabbitMQ connection found.")
    return {"conn_id": get_rabbitmq_conn_id()}
