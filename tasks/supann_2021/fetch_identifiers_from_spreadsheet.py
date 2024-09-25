from airflow.decorators import task

from utils.config import get_env_variable
from utils.spreadsheet import read_spreadsheet

# TODO: refacto all the *from_spreadsheet tasks to one task who can accept all three types of usecases

@task(task_id="fetch_identifiers_from_spreadsheet")
def fetch_identifiers_from_spreadsheet() -> dict[str, dict[str, str]]:
    """
    Fetch the identifiers from a spreadsheet.

    Returns:
        dict: A dict of identifiers with dn as key and a dict with 'identifier'
    """
    expected_columns = [
        "local",
        "id_hal_i",
        "id_hal_s",
        "orcid",
        "idref",
        "scopus_eid",
    ]

    data_path = get_env_variable("SPREADSHEET_IDENTIFIERS_PATH")

    df = read_spreadsheet(data_path, expected_columns)

    rows = df.to_dict(orient='records')

    return rows
