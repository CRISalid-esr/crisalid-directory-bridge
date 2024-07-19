import json
import logging

from airflow.decorators import task

from utils.rabbitmq import get_rabbitmq_hook

logger = logging.getLogger(__name__)


@task
def send_status_messages(entities_with_statuses: list[dict], entity_type: str) -> list[dict]:
    """
    Broadcast the structure data to the target system.

    :param entities_with_statuses: the structure data with statuses
    :param entity_type: the type of the entity
    :return: the messages sent
    """
    return [_send_status_message(e, entity_type) for e in entities_with_statuses]


def _send_status_message(entity_with_status: dict, entity_type: str) -> dict:
    status = entity_with_status['status']
    data = entity_with_status['data']
    hook = get_rabbitmq_hook()
    message = {
        'exchange': 'directory',
        'routing_key': f"event.directory.{entity_type}.{status}",
        'message': json.dumps(data, default=str)
    }
    hook.publish(**message)
    return message
