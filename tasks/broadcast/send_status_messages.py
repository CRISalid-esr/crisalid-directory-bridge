import json
import logging

from airflow.decorators import task
from pika import BlockingConnection
from pika.exchange_type import ExchangeType

from utils.config import get_env_variable
from utils.rabbitmq import get_rabbitmq_hook

logger = logging.getLogger(__name__)

PREFIXES = {
    "people": "AMQP_PEOPLE_MESSAGE_PREFIX",
    "structures": "AMQP_STRUCTURES_MESSAGE_PREFIX",
}


@task
def send_status_messages(entities_with_statuses: list[dict], entity_type: str) -> list[dict]:
    """
    Broadcast the structure data to the target system.

    :param entities_with_statuses: the structure data with statuses
    :param entity_type: the type of the entity
    :return: the messages sent
    """
    _create_exchange()
    return [_send_status_message(e, entity_type) for e in entities_with_statuses]


def _send_status_message(entity_with_status: dict, entity_type: str) -> dict:
    assert entity_type in PREFIXES, f"No message prefix found for entity type {entity_type}"
    prefix = get_env_variable(PREFIXES[entity_type])
    status = entity_with_status['status']
    data = entity_with_status['data']
    wrapper = {
        f"{entity_type}_event": {
            "type": status,
            "data": data,
        }
    }
    hook = get_rabbitmq_hook()
    message = {
        'exchange': 'directory',
        'routing_key': f"{prefix}{status}",
        'message': json.dumps(wrapper, default=str)
    }
    hook.publish(**message)
    return message


def _create_exchange():
    """
    Create the exchange in RabbitMQ.
    """
    hook = get_rabbitmq_hook()
    connection: BlockingConnection = hook.get_conn()
    channel = connection.channel()
    channel.exchange_declare(exchange='directory',
                             exchange_type=ExchangeType.TOPIC,
                             durable=True,
                             passive=False,
                             auto_delete=False,
                             internal=False)
    logger.info("Created exchange 'directory'")
