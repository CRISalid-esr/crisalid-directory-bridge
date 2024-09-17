import logging

from airflow.decorators import task

from read_spreadsheet import read_spreadsheet
from utils.config import get_env_variable

logger = logging.getLogger(__name__)


@task
def fetch_people_from_spreadsheet():
    """Fetch people from a spreadsheet.

    Returns:
        list: A list of rows from the spreadsheet.
    """

    columns_to_return = [
        "first_name",
        "last_name",
        "main_laboratory_identifier",
        "local_identifier",
        "idHal_i",
        "idHal_s",
        "orcid",
        "idref",
        "scopus_eid",
    ]

    data_path = get_env_variable("SPREADSHEET_PEOPLE_PATH")

    df = read_spreadsheet(data_path, columns_to_return)

    rows = df.to_dict(orient='records')

    return rows
