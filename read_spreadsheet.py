from collections import defaultdict

import pandas as pd

from utils.config import get_env_variable
from utils.exceptions import SPREADSHEETParseError


def read_spreadsheet(data_path, columns_to_return):
    """Fetch content from a spreadsheet.

    Returns:
        df: A pandas dataframe.
    """
    types = defaultdict(lambda: 'str')
    try:
        df = pd.read_csv(data_path, usecols=columns_to_return, dtype=types)
    except (FileNotFoundError, ValueError) as error:
        raise SPREADSHEETParseError(f"Error reading the spreadsheet: {str(error)}") from error

    df = df.fillna("")

    return df


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


def fetch_structures_from_spreadsheet():
    """Fetch structure from a spreadsheet.

    Returns:
        list: A list of rows from the structure spreadsheet.
    """

    expected_columns = [
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

    df = read_spreadsheet(data_path, expected_columns)

    rows = df.to_dict(orient='records')

    return rows
