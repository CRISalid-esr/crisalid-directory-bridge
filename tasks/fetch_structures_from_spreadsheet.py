import logging

from airflow.decorators import task

from read_spreadsheet import fetch_structures_from_spreadsheet

logger = logging.getLogger(__name__)


@task
def _fetch_structures_from_spreadsheet():
    """
    The logic from this function has been exported to allow it to be tested.
    """

    rows = fetch_structures_from_spreadsheet()

    return rows
