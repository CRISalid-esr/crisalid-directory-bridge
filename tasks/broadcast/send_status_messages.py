import json
import logging

from airflow.decorators import task

from utils.rabbitmq import get_rabbitmq_hook

logger = logging.getLogger(__name__)


@task
def send_status_messages(structure_data_with_status: dict):
    """
    Broadcast the structure data to the target system.

    :param structure_data_with_scores: the structure data with scores
    """
    status = structure_data_with_status['status']
    data = structure_data_with_status['data']
    hook = get_rabbitmq_hook()
    hook.publish(
        exchange='directory',
        routing_key=f"event.directory.structure.{status}",
        message=json.dumps(data, default=str)
    )
