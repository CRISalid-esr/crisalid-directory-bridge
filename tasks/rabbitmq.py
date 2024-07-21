import logging

from airflow.decorators import task
from airflow.models import Connection

from utils.rabbitmq import get_rabbitmq_conn_id, get_rabbitmq_hook, \
    create_rabbitmq_managed_connection

logger = logging.getLogger(__name__)


@task
def create_rabbitmq_connection() -> Connection:
    """
    Get or create the Airflow managed RabbitMQ connection.
    :return: The RabbitMQ connection
    """
    connexion = get_rabbitmq_hook()
    logger.info("**** Existing RabbitMQ connection ****")
    logger.info("RabbitMQ connection: %s", connexion)
    if connexion is None:
        logger.info("**** Creating RabbitMQ connection ****")
        create_rabbitmq_managed_connection()
    return {"conn_id": get_rabbitmq_conn_id()}
