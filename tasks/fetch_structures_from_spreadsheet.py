import logging

from airflow.decorators import task

from read_spreadsheet import read_spreadsheet
from utils.config import get_env_variable

from read_spreadsheet import fetch_structures_from_spreadsheet as get_structure_rows

logger = logging.getLogger(__name__)


@task
def fetch_structures_from_spreadsheet():

    rows = get_structure_rows()

    return rows
