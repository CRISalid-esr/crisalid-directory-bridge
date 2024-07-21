import json
import logging

from airflow.decorators import task
from airflow.models import Connection
from pendulum import DateTime

from utils.redis import get_redis_client, \
    get_redis_conn_id, \
    get_redis_connection, \
    create_redis_managed_connection

logger = logging.getLogger(__name__)


@task(task_id="read_entity_keys_from_redis")
def read_entity_keys_from_redis(entity_type: str, entity_source: str) -> list[str]:
    """
    Read structures from Redis.

    :param entity_type: the type of the entity e.g. 'structures'
    :param entity_source: the source of the entity e.g 'ldap'
    :return: List of keys of the sorted sets in Redis
    """
    prefix = f"{entity_type}:{entity_source}:"
    client = get_redis_client()
    logger.info("Reading keys from Redis with prefix: %s", prefix)
    keys = client.keys(f"{prefix}*")
    return [key.decode('utf-8') for key in keys]


@task
def read_entities_with_scores_from_redis(entity_keys: list[str], timestamp: str) -> list[dict]:
    """
    Read structures from Redis.

    :param entity_keys: list of keys of the sorted set in Redis
    :param arguments: dict with the timenstand and entity type and source
    :return: List of dictionaries of the 2 most recent records of the structure with the scores
    """
    client = get_redis_client()
    entities_with_scores: list[dict] = []
    for redis_key in entity_keys:
        data = client.zrevrangebyscore(
            redis_key, timestamp, '-inf', start=0, num=2, withscores=True
        )
        entities_with_scores.append(
            {str(int(d[1])): json.loads(d[0].decode('utf-8')) for d in data}
        )
    return entities_with_scores


@task
def update_database(result: dict, prefix: str, **kwargs) -> list[str]:
    """
    Update the database with the result of a task.

    :param result: the converted result
    :param prefix: the prefix to use for the Redis key
    :param kwargs:
    :return:
    """
    redis_keys = []
    for entry in result.values():
        logger.debug("Updating database with entry: %s", entry)
        date: DateTime = kwargs.get('data_interval_start')
        timestamp = date.int_timestamp
        client = get_redis_client()
        identifier = next(
            (i['value'] for i in entry.get('identifiers', []) if i.get('type') == 'local'),
            None
        )
        assert identifier is not None, f"Identifier is None in {entry}"
        redis_key = f"{prefix}{identifier}"
        serialized_entry = json.dumps({"data": entry, "timestamp": timestamp})
        client.zadd(redis_key, {serialized_entry: timestamp})
        redis_keys.append(redis_key)
    return redis_keys


@task
def create_redis_connection() -> Connection:
    """
    Create an Airflow managed Redis connection.
    :return: the connection
    """
    connection = get_redis_connection()
    logger.info("*** Connection ***")
    logger.info(connection)
    if connection is None:
        create_redis_managed_connection()
    return {"conn_id": get_redis_conn_id()}
