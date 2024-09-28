from collections import defaultdict

import pandas as pd

from utils.exceptions import SpreadSheetParseError


def read_spreadsheet(data_path, columns_to_return):
    """Fetch content from a spreadsheet.

    Returns:
        df: A pandas dataframe.
    """
    types = defaultdict(lambda: 'str')
    try:
        df = pd.read_csv(data_path, usecols=columns_to_return, dtype=types)
    except (FileNotFoundError, ValueError) as error:
        raise SpreadSheetParseError(f"Error reading the spreadsheet: {str(error)}") from error

    df = df.fillna("")

    return df
