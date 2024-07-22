import logging
from collections import defaultdict

from airflow.decorators import task

logger = logging.getLogger(__name__)


@task
def combine_batch_results(batch_results):
    """
    Combine batch results from field converters into a single dictionary.
    :param batch_results: List of dictionaries containing the results of the field converters
    :return: A single dictionary containing the combined results
    """
    combined_results = defaultdict(dict)
    for batch in batch_results:
        logger.debug("Combining batch results: %s", batch)
        for dn, entry in batch.items():
            combined_results[dn].update(entry)
    return combined_results
