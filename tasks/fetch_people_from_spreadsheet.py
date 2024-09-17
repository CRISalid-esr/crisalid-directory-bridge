import logging

from airflow.decorators import task

from read_spreadsheet import fetch_people_from_spreadsheet as get_people_rows

logger = logging.getLogger(__name__)


@task
def fetch_people_from_spreadsheet():

    rows = get_people_rows()

    return rows
