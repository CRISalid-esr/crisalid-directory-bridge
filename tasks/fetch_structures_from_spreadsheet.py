import logging

import pandas as pd
from airflow.decorators import task


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
        "city_name",
        "city_code",
        "city_adress",
    ]

    df = pd.read_csv("structure.csv", usecols=columns_to_return)

    df = df.fillna("")

    rows = df.to_dict(orient='records')

    return rows
