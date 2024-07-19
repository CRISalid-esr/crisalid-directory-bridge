import logging

from airflow.decorators import task
from deepdiff import DeepDiff

logger = logging.getLogger(__name__)


@task
def compute_entity_statuses(entities_with_scores: list[dict], timestamp: str) -> list[dict]:
    """
    Computes the events based on the comparaison of the two last states

    :param entities_with_scores: the structure data with scores
    :param timestamp: the timestamp of the last fetched data

    """
    return [_compute_entity_status(entity, timestamp) for entity in entities_with_scores]


def _compute_entity_status(entity_with_scores: dict, timestamp: str) -> dict:
    status = None
    assert all(int(key) <= int(timestamp) for key in
               entity_with_scores.keys()), \
        f"There is a state with a timestamp greater than the current timestamp {timestamp}"
    # if we have a state with the current timestamp
    # if we don't have a previous state, status is "created"
    # if we have a previous state, and data is the same, status is "unchanged"
    # if we have a previous state, and data is different, status is "updated"
    if timestamp in entity_with_scores:
        current_state = entity_with_scores[timestamp]['data']
        # look for a previous state
        previous_state = None
        for key in entity_with_scores.keys():
            if int(key) < int(timestamp):
                previous_state = entity_with_scores[key]['data']
                break
        if previous_state is None:
            status = "created"
        else:
            if not DeepDiff(previous_state, current_state, ignore_order=True).to_dict():
                status = "unchanged"
            else:
                status = "updated"
    else:
        # if we have at least one previous state, status is "deleted"
        status = "deleted"

    return {"status": status, "data": entity_with_scores[timestamp]['data']}
