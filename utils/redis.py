import logging

import redis
from airflow.exceptions import AirflowNotFoundException
from airflow.hooks.base import BaseHook
from airflow.models import Connection
from airflow.utils.session import provide_session

from utils.config import get_env_variable

logger = logging.getLogger(__name__)


def get_redis_client():
    """
    Get a Redis client from the Airflow Redis connection.
    :return:
    """
    connexion = get_redis_connection()
    assert connexion is not None, "Create Redis connection before using it"
    client = redis.StrictRedis(
        host=connexion.host,
        port=connexion.port,
        password=connexion.password
    )
    return client


def get_redis_connection() -> Connection | None:
    """
    Get the Airflow managed Redis connection if it exists.
    :return:
    """
    redis_conn_id = get_redis_conn_id()
    try:
        connexion = BaseHook.get_connection(redis_conn_id)
    except AirflowNotFoundException as e:
        logger.warning("No existing connection found: %s", str(e))
        return None
    return connexion


def get_redis_conn_id() -> str:
    """
    Get the Redis connection ID from the environment variables.
    :return: The Redis connection ID
    """
    redis_conn_id = get_env_variable("REDIS_CONN_ID")
    assert redis_conn_id is not None, "No Redis connection ID found"
    return redis_conn_id


@provide_session
def create_redis_managed_connection(session=None) -> None:
    """
    Create an Airflow managed Redis connection.
    :param session: The SQLAlchemy session
    :return: None
    """
    redis_conn_id = get_redis_conn_id()
    try:
        logger.info("Creating connection: %s without password", redis_conn_id)
        connection = Connection(
            conn_id=redis_conn_id,
            conn_type='redis',
            host=get_env_variable("REDIS_HOST"),
            port=get_env_variable("REDIS_PORT"),
            # password=get_env_variable("REDIS_PASSWORD"),
        )
        logger.info("Connection object: %s", connection)
        logger.info("Connection host: %s", connection.host)
        logger.info("Connection port: %s", connection.port)
        logger.info("Testing connectivity")
        client = redis.StrictRedis(
            host=connection.host,
            port=connection.port,
            password=connection.password
        )
        ping_result = client.ping()
        logger.info("Ping result: %s", ping_result)
        if ping_result != b'PONG':
            logger.error("Failed to ping Redis server : %s", ping_result)
            raise Exception("Failed to ping Redis server")
        session.add(connection)
        session.commit()
        logger.info("Successfully created connection: %s", redis_conn_id)
    except Exception as e:
        logger.info("Failed to create connection: %s", str(e))
        session.rollback()
        raise e
    finally:
        if session:
            session.close()
