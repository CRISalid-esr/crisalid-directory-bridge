# utils/redis.py
import logging

import redis
from airflow.exceptions import AirflowNotFoundException
from airflow.hooks.base import BaseHook
from airflow.models import Connection

from utils.config import get_env_variable

logger = logging.getLogger(__name__)


def get_redis_client() -> redis.StrictRedis:
    """
    Create a Redis client from an Airflow-managed connection.
    :param conn_id: Airflow connection ID (must match AIRFLOW_CONN_<ID>)
    :return: redis.StrictRedis client
    """
    try:
        conn = BaseHook.get_connection(get_redis_conn_id())
    except Exception as e:
        logger.error("Failed to retrieve Redis connection %s: %s", get_redis_conn_id(), str(e))
        raise

    client_params = {
        "host": conn.host,
        "port": conn.port,
    }
    if conn.password:
        client_params["password"] = conn.password

    client = redis.StrictRedis(**client_params)
    return client


def get_redis_managed_connection() -> Connection | None:
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
    redis_conn_id = get_env_variable("CDB_REDIS_CONN_ID")
    assert redis_conn_id is not None, "No Redis connection ID found"
    return redis_conn_id
