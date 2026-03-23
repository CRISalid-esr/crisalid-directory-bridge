import logging

from airflow.sdk import task

from utils.dates import is_valid_iso_date

logger = logging.getLogger(__name__)

LOCAL_PERSON_IDENTIFIER = 'tracking_id'

VALID_MEMBERSHIP_TYPES = {'stat_mmb', 'assoc_mmb', 'second_mmb', 'visit_mmb'}
DEFAULT_MEMBERSHIP_TYPE = 'stat_mmb'

PERSON_IDENTIFIERS = [
    LOCAL_PERSON_IDENTIFIER,
    'eppn',
    'idhali',
    'idhals',
    'orcid',
    'idref',
    'scopus',
    'researcherid'
]

IDENTIFIER_TYPE_MAP = {
    'tracking_id': 'local',
    'idhali': 'idhali',
    'idhals': 'idhals',
}


def extract_identifiers(row_data: dict[str, str]) -> list[dict[str, str]]:
    """
    Extract all non-empty person identifiers from a spreadsheet row.

    This function scans the predefined PERSON_IDENTIFIERS list and collects all
    identifiers that are present and non-empty, mapping them to their corresponding type.

    Args:
        row_data (dict): A single row from the source spreadsheet data.

    Returns:
        list[dict[str, str]]: A list of dictionaries containing each identifier's
        type and value, e.g.:
        [
            {"type": "orcid", "value": "0000-0002-1825-0097"},
            {"type": "local", "value": "12345"}
        ]
    """
    return [
        {
            'type': IDENTIFIER_TYPE_MAP.get(identifier, identifier),
            'value': row_data[identifier]
        }
        for identifier in PERSON_IDENTIFIERS
        if row_data.get(identifier) and row_data[identifier].strip()
    ]


def extract_membership_type(row_data: dict, person_id: str) -> str:
    """
    Extract and validate membership type from spreadsheet row.

    Args:
        row_data (dict): The spreadsheet row containing membership type.
        person_id (str): Identifier of the person.

    Returns:
        str: One of the valid membership types (stat_mmb, assoc_mmb, second_mmb, visit_mmb).
             Defaults to stat_mmb if not provided or invalid.

    Logs a warning if an invalid membership type is provided.
    """
    membership_type = row_data.get('membership_type', '').strip().lower()

    if not membership_type:
        return DEFAULT_MEMBERSHIP_TYPE

    if membership_type in VALID_MEMBERSHIP_TYPES:
        return membership_type

    logger.warning(
        "Invalid membership_type '%s' for person %s. Valid types are: %s. "
        "Defaulting to %s.",
        membership_type, person_id, ', '.join(VALID_MEMBERSHIP_TYPES), DEFAULT_MEMBERSHIP_TYPE
    )
    return DEFAULT_MEMBERSHIP_TYPE


def extract_contact_email(row_data: dict, person_id: str) -> str | None:
    """
    Extract contact email from spreadsheet row.

    Args:
        row_data (dict): The spreadsheet row containing contact email.
        person_id (str): Identifier of the person.

    Returns:
        str | None: The contact email if provided, None otherwise.
    """
    contact_email = row_data.get('contact_email', '').strip()
    
    if not contact_email:
        return None
    
    logger.debug("Contact email found for person %s: %s", person_id, contact_email)
    return contact_email


def extract_auth_email(row_data: dict, person_id: str) -> str | None:
    """
    Extract authentication email from spreadsheet row.

    Args:
        row_data (dict): The spreadsheet row containing auth email.
        person_id (str): Identifier of the person.

    Returns:
        str | None: The authentication email if provided, None otherwise.
    """
    auth_email = row_data.get('auth_email', '').strip()
    
    if not auth_email:
        return None
    
    logger.debug("Auth email found for person %s: %s", person_id, auth_email)
    return auth_email


def extract_membership_dates(row_data: dict, person_id: str) -> dict[str, str]:
    """
    Extract and validate membership start and end dates from spreadsheet row.

    Dates should be in ISO8601 format (YYYY-MM-DD).

    Args:
        row_data (dict): The spreadsheet row containing membership dates.
        person_id (str): Identifier of the person.

    Returns:
        dict[str, str]: A dictionary with "start_date" and/or "end_date" keys.
                       Empty dict if no valid dates provided.

    Logs a warning if any provided date has an invalid format.
    """
    dates = {}
    for date_key in ["membership_start_date", "membership_end_date"]:
        date_value = row_data.get(date_key, '').strip()
        if date_value:
            if is_valid_iso_date(date_value):
                dates[date_key.replace("membership_", "")] = date_value
            else:
                logger.warning(
                    "Invalid date format '%s' for person %s in field '%s'. "
                    "Expected format is YYYY-MM-DD. Skipping this date.",
                    date_value, person_id, date_key
                )
    return dates


def extract_employment_position(row_data: dict,
                                bodies_labels_dict: dict[str, str],
                                person_id: str) -> dict[str, str]:
    """
    Build a position dictionary based on the 'position' field.

    If the 'position' value exists and matches an entry in the YAML mapping,
    return a dict formatted for LDAP with title and code. Otherwise,
    return an empty dict.

    Args:
        row_data (dict): A single row from the spreadsheet data.
        bodies_labels_dict (dict[str, str]): Mapping of corps codes to labels
        person_id (str): Identifier of the person.

    Returns:
        dict[str, str]: A position dictionary {"title": ..., "code": ...} or an empty dict.
    """
    body = row_data.get('position', '').strip()

    if not body:
        return {}

    if body in bodies_labels_dict:
        title = bodies_labels_dict[body]
        return {
            "title": title,
            "code": body
        }

    logger.warning(
        "Position '%s' not found in YAML mapping for person %s",
        body, person_id
    )
    return {}


def extract_employment_dates(row_data: dict, person_id: str) -> dict[str, str]:
    """
    Extract and validate employment start and end dates.

    Args:
        row_data (dict): The spreadsheet row containing employment dates.
        person_id (str): Identifier of the person.

    Returns:
        dict[str, str]: A dictionary with "start_date" and/or "end_date" keys.

    Raises:
        ValueError: If any provided date has an invalid format.
    """
    dates = {}
    for date_key in ["employment_start_date", "employment_end_date"]:
        date_value = row_data.get(date_key, '').strip()
        if date_value:
            if is_valid_iso_date(date_value):
                dates[date_key.replace("employment_", "")] = date_value
            else:
                raise ValueError(
                    f"Invalid date format '{date_value}' for person {person_id} "
                    f"in field '{date_key}'. Expected format is YYYY-MM-DD."
                )
    return dates


def extract_employment_hdr(row_data: dict, person_id: str) -> bool:
    """
    Determine and normalize the HDR ("Habilitation à Diriger des Recherches") value.

    Args:
        row_data (dict): The spreadsheet row containing HDR information.
        person_id (str): Identifier of the person.

    Returns:
        bool: True if HDR is "yes", False if "no" or unrecognized.

    Logs a warning if the HDR value is unrecognized and defaults to False.
    """
    hdr_value = row_data.get('hdr', '').strip().lower()
    if hdr_value in ["yes", "no"]:
        return hdr_value == "yes"

    if hdr_value == "":
        return False

    logger.warning(
        "Invalid HDR value '%s' for person %s. Defaulting to False.",
        hdr_value,
        person_id
    )
    return False


def _build_employment(entry: dict[str, str],
                      bodies_labels_dict: dict[str, str]) -> dict[str, str | dict]:
    """
    Build and validate an employment structure for a person.

    Constructs a standardized employment dictionary, validating
    required fields, checking dates, and attaching the position when applicable.

    Args:
        entry (dict): The source spreadsheet entry.
        bodies_labels_dict (dict): Mapping of corps codes to labels

    Returns:
        dict[str, str | dict]: A structured employment dictionary.
        Returns an empty dict if no institution_id is provided.

    Raises:
        ValueError: If mandatory fields are missing or invalid.
    """
    person_id = entry.get(LOCAL_PERSON_IDENTIFIER)
    institution_id = entry.get('institution_identifier', '').strip()
    institution_id_nomenclature = entry.get('institution_id_nomenclature', '').strip()

    if not institution_id:
        return {}

    if not institution_id_nomenclature:
        raise ValueError(
            f"Field institution_id_nomenclature is required "
            f"when institution_id is provided for person {person_id}.")

    if institution_id_nomenclature and institution_id_nomenclature not in {'UAI', 'ROR'}:
        raise ValueError(
            f"Invalid institution_id_nomenclature '{institution_id_nomenclature}' "
            f"for person {person_id}. Must be 'UAI' or 'ROR'.")

    # Determine the prefix based on institution_id_nomenclature
    prefix = "uai-" if institution_id_nomenclature.upper() == "UAI" else "ror-"

    dates = extract_employment_dates(entry, person_id)
    position = extract_employment_position(entry, bodies_labels_dict, person_id)

    employment: dict[str, str | dict] = {
        "entity_uid": f"{prefix}{institution_id}",
        "hdr": extract_employment_hdr(entry, person_id)
    }

    if "start_date" in dates:
        employment["start_date"] = dates["start_date"]
    if "end_date" in dates:
        employment["end_date"] = dates["end_date"]

    if position:
        employment["position"] = position
    return employment


@task(task_id="convert_spreadsheet_people")
def convert_spreadsheet_people(
        source_data: list[dict[str, str]],
        config: dict[str, str]
) -> dict[str, dict[str, str | dict]]:
    """
    Convert raw spreadsheet rows into structured person records.

    This Airflow task transforms spreadsheet data into a standardized structure
    with names, identifiers, memberships, and employment data.

    Args:
        source_data (list[dict[str, str]]): A list of spreadsheet rows, each row
            represented as a dictionary.
        config (dict[str, str]): Configuration mapping for employment positions.

    Returns:
        dict[str, dict[str, str | dict]]: A dictionary keyed by the LOCAL_PERSON_IDENTIFIER
        (e.g., tracking_id), where each value is a structured person record.
    """
    task_results = {}
    required_fields = [LOCAL_PERSON_IDENTIFIER, 'last_name', 'first_names']

    for entry in source_data:
        for field in required_fields:
            if not entry.get(field):
                raise ValueError(f"Missing required field '{field}' in row: {entry}")

        non_empty_identifiers = extract_identifiers(entry)
        if not non_empty_identifiers:
            logger.warning("No identifiers for row: %s", entry)

        entity_uid = entry.get('main_research_structure', '').strip()
        person_id = entry.get(LOCAL_PERSON_IDENTIFIER)

        memberships = []
        if entity_uid:
            membership_type = extract_membership_type(entry, person_id)
            membership_dates = extract_membership_dates(entry, person_id)
            
            membership = {'entity_uid': entity_uid, 'membership_type': membership_type}
            
            # Add dates if provided
            if 'start_date' in membership_dates:
                membership['start_date'] = membership_dates['start_date']
            if 'end_date' in membership_dates:
                membership['end_date'] = membership_dates['end_date']
            
            memberships = [membership]

        result_entry = {
            'names': [
                {
                    'last_names': [{'value': entry['last_name'], 'language': 'fr'}],
                    'first_names': [
                        {'value': first_name, 'language': 'fr'}
                        for first_name in entry.get('first_names', '').split(',')
                    ]
                }
            ],
            'identifiers': non_empty_identifiers,
            'memberships': memberships,
        }

        employment = _build_employment(entry, config)
        if employment:
            result_entry['employments'] = [employment]

        # Add emails if provided
        contact_email = extract_contact_email(entry, person_id)
        if contact_email:
            result_entry['contact_email'] = contact_email

        auth_email = extract_auth_email(entry, person_id)
        if auth_email:
            result_entry['auth_email'] = auth_email

        task_results[entry[LOCAL_PERSON_IDENTIFIER]] = result_entry

    return task_results
