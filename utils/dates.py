from datetime import datetime


def is_valid_iso_date(date_str: str) -> bool:
    """
    Check if a string is a valid ISO 8601 date (YYYY-MM-DD).

    Args:
        date_str (str): The date string to validate.

    Returns:
        bool: True if valid, False otherwise.
    """
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except ValueError:
        return False
