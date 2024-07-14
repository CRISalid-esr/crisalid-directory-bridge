from airflow.decorators import task
from airflow.models import Connection

from utils.rabbitmq import create_rabbitmq_connection, get_rabbitmq_conn_id


@task
def create_rabbitmq_connection_task() -> Connection:
    """
    Get or create the Airflow managed RabbitMQ connection.
    :return: The RabbitMQ connection
    """
    create_rabbitmq_connection()
    return {"conn_id": get_rabbitmq_conn_id()}
