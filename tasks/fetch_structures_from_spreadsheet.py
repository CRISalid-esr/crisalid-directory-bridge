import logging

from airflow.decorators import task

from utils.config import get_env_variable
from utils.spreadsheet import read_spreadsheet

logger = logging.getLogger(__name__)


@task
def fetch_structures_from_spreadsheet():
    """Fetch structure from a spreadsheet.

    Returns:
        list: A list of rows from the structure spreadsheet.
    """

    expected_columns = [
        "name",
        "acronym",
        "description",
        "local",
        "rnsr",
        "ror",
        "city_name",
        "city_code",
        "city_adress",
    ]

    data_path = get_env_variable("SPREADSHEET_STRUCTURES_PATH")

    df = read_spreadsheet(data_path, expected_columns)

    rows = df.to_dict(orient='records')

    return rows
