import json
import logging

from airflow.sdk import task
from pika.adapters.blocking_connection import BlockingChannel

from utils.config import get_env_variable
from utils.rabbitmq import open_pika_channel

logger = logging.getLogger(__name__)

PREFIXES = {
    "people": "AMQP_PEOPLE_MESSAGE_PREFIX",
    "structures": "AMQP_STRUCTURES_MESSAGE_PREFIX",
}


@task
def send_status_messages(entities_with_statuses: list[dict], entity_type: str) -> list[dict]:
    """
    Publish status messages to RabbitMQ exchange (topic).
    """
    connection, channel = open_pika_channel()
    try:
        out: list[dict] = []
        for e in entities_with_statuses:
            out.append(_send_status_message(channel, e, entity_type))
        return out
    finally:
        try:
            channel.close()
        except Exception:  # pylint: disable=broad-exception-caught
            logger.exception("Error closing channel")
        try:
            connection.close()
        except Exception:  # pylint: disable=broad-exception-caught
            logger.exception("Error closing connection")


def _send_status_message(channel: BlockingChannel,
                         entity_with_status: dict, entity_type: str) -> dict:
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
    params = {
        'exchange': 'directory',
        'routing_key': f"{prefix}{status}",
        'body': json.dumps(wrapper, default=str)
    }
    channel.basic_publish(**params)
    return params
