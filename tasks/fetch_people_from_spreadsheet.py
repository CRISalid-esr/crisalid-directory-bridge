import logging

from airflow.decorators import task

from utils.config import get_env_variable

logger = logging.getLogger(__name__)


@task
def fetch_people_from_spreadsheet():
    """Fetch people from a spreadsheet.

    Returns:
        list: A list of rows from the spreadsheet.
    """
    # people_spreadsheet_path = get_env_variable("PEOPLE_SPREADSHEET_PATH")
    # load the spreadsheet from the path
    # return the rows

    rows = [
        {"uid": "test1",
         "name": "Test User 1",
         "idref": "123456",
         "idhal": "654321",
         "orcid": "0000-0000-0000-0000",
         "email": "testuser@univ.fr"},
    ]

    return rows
