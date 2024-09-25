def ensure_list(a) -> list:
    """
    Ensure that the input is a list.
    :param a: the input, may be a list or not
    :return: a list
    """
    return a if isinstance(a, list) else [a]


def deduplicate_dict_list(list_: list[dict]) -> list[dict]:
    """
    Deduplicate a list of dictionaries.
    :param list_: a list of dictionaries
    :return: the deduplicated list
    """
    return [dict(t) for t in {tuple(d.items()) for d in list_}]
