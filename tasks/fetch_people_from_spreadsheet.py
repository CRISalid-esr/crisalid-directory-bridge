import logging

import pandas as pd
from airflow.decorators import task

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
        "main_laboratory_supann",
        "local_identifier",
        "orcid",
        "idref",
    ]

    df = pd.read_csv("people.csv", usecols=columns_to_return)

    df = df.fillna("")

    rows = df.to_dict(orient='records')

    return rows
