import logging
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from utils.rabbitmq import get_rabbitmq_conn_id

logger = logging.getLogger(__name__)

@task
def ensure_rabbitmq_connection() -> dict:
    """
    Fail early if the Airflow Connection cannot be resolved.
    """
    conn_id = get_rabbitmq_conn_id()
    conn = BaseHook.get_connection(conn_id)  # raises if not found/misconfigured
    logger.info("Resolved RabbitMQ connection: %s@%s:%s vhost=%s",
                conn.login, conn.host, conn.port, conn.schema or "")
    return {"conn_id": conn_id}
