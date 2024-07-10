import json
import logging

from airflow.decorators import task
from airflow.models import Connection
from pendulum import DateTime

from utils.redis import get_redis_client, \
    get_redis_conn_id, \
    get_redis_connection, \
    create_redis_connection

STRUCTURE_PREFIX = "struct:ldap:"

logger = logging.getLogger(__name__)


@task(task_id="read_structures_from_redis")
def read_structure_keys_from_redis() -> list:
    """
    Read structures from Redis.

    :param kwargs:
    :return:
    """
    client = get_redis_client()
    keys = client.keys(f"{STRUCTURE_PREFIX}*")
    return [key.decode('utf-8') for key in keys]


@task
def read_structure_with_scores_from_redis(redis_key: str, **kwargs) -> dict:
    """
    Read structures from Redis.

    :param redis_key: Key of the sorted set in Redis
    :return: Dictionary of the 2 most recent records of the structure with the scores
    """
    timestamp = kwargs['dag_run'].conf.get('timestamp')
    client = get_redis_client()
    # Get top 2 highest scores from the sorted set less or equal to the timestamp
    data = client.zrevrangebyscore(redis_key, timestamp, '-inf', start=0, num=2, withscores=True)
    return {str(int(d[1])): json.loads(d[0].decode('utf-8')) for d in data}


@task
def update_database_task(result: dict, **kwargs) -> str:
    """
    Update the database with the result of a task.

    :param result: the converted result
    :param conn_id: the connection id to the Redis database
    :param kwargs:
    :return:
    """
    date: DateTime = kwargs.get('data_interval_start')
    timestamp = date.int_timestamp

    client = get_redis_client()
    identifier = result.get('identifier', None)
    assert identifier is not None, f"Identifier is None in {result}"
    redis_key = f"{STRUCTURE_PREFIX}{identifier}"
    serialized_result = json.dumps({"data": result, "timestamp": timestamp})
    client.zadd(redis_key, {serialized_result: timestamp})
    return redis_key


@task
def create_redis_connection_task() -> Connection:
    """
    Create an Airflow managed Redis connection.
    :return: the connection
    """
    connection = get_redis_connection()
    if connection is None:
        create_redis_connection()
    return {"conn_id": get_redis_conn_id()}
