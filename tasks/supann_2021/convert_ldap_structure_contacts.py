from airflow.decorators import task


@task(task_id="convert_ldap_structure_contacts")
def convert_ldap_structure_contacts(ldap_results: dict[str, dict[str, str | dict]]) \
        -> dict[str, list[dict]]:
    """
    Extract and parse the 'postalAddress' field from a dict of LDAP entries.

    Args:
        ldap_results (dict): A dict of LDAP results with dn as key and entry as value.

    Returns:
        dict: A dict with dn as key and a list containing a
        single dictionary with the parsed address as value.
    """
    task_results = {}
    for dn, ldap_entry in ldap_results.items():
        assert ldap_entry is not None, f"LDAP entry is None for dn: {dn}"

        postal_addresses = ldap_entry.get('postalAddress', '')
        if not isinstance(postal_addresses, list) or not postal_addresses:
            task_results[dn] = {"contacts": []}
            continue
        postal_address = postal_addresses[0]
        lines = postal_address.split('$')
        try:
            address_dict = _parse_address_lines(lines)

            task_results[dn] = {"contacts": [{
                'type': 'postal_address',
                'format': 'structured_physical_address',
                'value': address_dict
            }]}
        except (ValueError, IndexError):
            task_results[dn] = {"contacts": [{
                'type': 'postal_address',
                'format': 'simple_physical_address',
                'value': {'address': postal_address.replace('$', '\n')}
            }]}

    return task_results


def _parse_address_lines(lines):
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
    return address_dict
