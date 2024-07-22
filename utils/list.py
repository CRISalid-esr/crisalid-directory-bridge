def ensure_list(a) -> list:
    """
    Ensure that the input is a list.
    :param a: the input, may be a list or not
    :return: a list
    """
    return a if isinstance(a, list) else [a]
