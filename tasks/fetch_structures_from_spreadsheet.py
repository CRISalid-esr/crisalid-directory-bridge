import logging

from airflow.decorators import task

from read_spreadsheet import read_spreadsheet
from utils.config import get_env_variable

logger = logging.getLogger(__name__)


@task
def fetch_structures_from_spreadsheet():
    """Fetch structure from a spreadsheet.

    Returns:
        list: A list of rows from the spreadsheet.
    """

    columns_to_return = [
        "name",
        "acronym",
        "description",
        "local_identifier",
        "RNSR",
        "ROR",
        "city_name",
        "city_code",
        "city_adress",
    ]

    data_path = get_env_variable("SPREADSHEET_STRUCTURES_PATH")

    df = read_spreadsheet(data_path, columns_to_return)

    rows = df.to_dict(orient='records')

    return rows
