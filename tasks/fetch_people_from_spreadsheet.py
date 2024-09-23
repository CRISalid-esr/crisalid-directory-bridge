import logging

from airflow.decorators import task

from utils.config import get_env_variable
from utils.spreadsheet import read_spreadsheet

logger = logging.getLogger(__name__)


@task
def fetch_people_from_spreadsheet():
    """Fetch people from a spreadsheet.

    Returns:
        list: A list of rows from the people spreadsheet.
    """

    expected_columns = [
        "first_name",
        "last_name",
        "main_laboratory_identifier",
        "local",
        "id_hal_i",
        "id_hal_s",
        "orcid",
        "idref",
        "scopus_eid",
    ]

    data_path = get_env_variable("SPREADSHEET_PEOPLE_PATH")

    df = read_spreadsheet(data_path, expected_columns)

    rows = df.to_dict(orient='records')

    return rows
