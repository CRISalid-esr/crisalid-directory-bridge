import logging

from airflow.decorators import task
from deepdiff import DeepDiff

logger = logging.getLogger(__name__)


@task
def compute_structure_status(structure_data_with_scores: dict, **kwargs):
    """
    Computes the events based on the comparaison of the two last states

    :param structure_data_with_scores: the structure data with scores
    """
    timestamp = kwargs['dag_run'].conf.get('timestamp')
    status = None
    assert all(int(key) <= int(timestamp) for key in
               structure_data_with_scores.keys()), \
        f"There is a state with a timestamp greater than the current timestamp {timestamp}"
    # if we have a state with the current timestamp
    # if we don't have a previous state, status is "created"
    # if we have a previous state, and data is the same, status is "unchanged"
    # if we have a previous state, and data is different, status is "updated"
    if timestamp in structure_data_with_scores:
        current_state = structure_data_with_scores[timestamp]['data']
        # look for a previous state
        previous_state = None
        for key in structure_data_with_scores.keys():
            if int(key) < int(timestamp):
                previous_state = structure_data_with_scores[key]['data']
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

    return {"status": status, "data": structure_data_with_scores[timestamp]['data']}
