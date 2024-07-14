from airflow.decorators import task


@task
def convert_ldap_structure_contacts_task(ldap_result: dict[str, str | dict]) -> list[dict]:
    """Extract and parse the 'postalAddress' field from an LDAP entry.

    Args:
        ldap_result (dict): An LDAP result with "dn" and "entry" fields.

    Returns:
        list[dict]: A list containing a single dictionary with the parsed address.
    """
    ldap_entry = ldap_result.get('entry', None)
    assert ldap_entry is not None, "LDAP entry is None"

    postal_address = ldap_entry.get('postalAddress', '')
    if not postal_address:
        return []

    lines = postal_address.split('$')

    try:
        address_dict = {}

        # Handle the country
        if not any(char.isdigit() for char in lines[-1]):
            address_dict["country"] = lines.pop()

        # Handle the city and zip code
        if any(char.isdigit() for char in lines[-1]):
            zip_city = lines.pop().split(maxsplit=1)
            if len(zip_city) != 2:
                raise ValueError("Invalid zip code and city format")
            address_dict["zip_code"], address_dict["city"] = zip_city

        # Handle the streets
        if lines:
            address_dict["street"] = lines.pop(0)
        if lines:
            address_dict["street"] += f", {' '.join(lines)}"

        return [{
            'type': 'postal_address',
            'format': 'structured_physical_address',
            'value': address_dict
        }]
    except (ValueError, IndexError):
        return [{
            'type': 'postal_address',
            'format': 'simple_physical_address',
            'value': {'address': postal_address.replace('$', '\n')}
        }]
