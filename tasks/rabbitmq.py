from airflow.decorators import task
from airflow.models import Connection

from utils.rabbitmq import get_rabbitmq_hook, create_rabbitmq_connection, get_rabbitmq_conn_id


@task
def create_rabbitmq_connection_task() -> Connection:
    """
    Get or create the Airflow managed RabbitMQ connection.
    :return: The RabbitMQ connection
    """
    hook = get_rabbitmq_hook()
    if hook is None:
        create_rabbitmq_connection()
    return {"conn_id": get_rabbitmq_conn_id()}
