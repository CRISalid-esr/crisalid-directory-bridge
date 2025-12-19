import logging

from airflow.decorators import task

from email_validator import validate_email, EmailNotValidError
from utils.dates import is_valid_iso_date

logger = logging.getLogger(__name__)

LOCAL_PERSON_IDENTIFIER = 'tracking_id'

PERSON_IDENTIFIERS = [
    LOCAL_PERSON_IDENTIFIER,
    'eppn',
    'idhal_i',
    'idhal_s',
    'orcid',
    'idref',
    'scopus_eid'
]

IDENTIFIER_TYPE_MAP = {
    'tracking_id': 'local',
    'idhal_i': 'id_hal_i',
    'idhal_s': 'id_hal_s',
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

    dates = extract_employment_dates(entry, person_id)
    position = extract_employment_position(entry, bodies_labels_dict, person_id)

    employment: dict[str, str | dict] = {
        "entity_uid": f"UAI-{institution_id}",
        "hdr": extract_employment_hdr(entry, person_id)
    }

    if "start_date" in dates:
        employment["start_date"] = dates["start_date"]
    if "end_date" in dates:
        employment["end_date"] = dates["end_date"]

    if position:
        employment["position"] = position
    return employment


def email_is_valid(email: str) -> bool:
    """
    Simple email validation function.

    Args:
        email (str): The email address to validate.

    Returns:
        bool: True if the email is valid, False otherwise.
    """
    try:
        validate_email(email, check_deliverability=False)
        return True
    except EmailNotValidError:
        return False


def validate_email_address(person_id: str, raw_email: str | None) -> str | None:
    """
    Validate an email address and log a warning if invalid.

    Args:
        raw_email (str): The email address to validate.
        person_id (str): Identifier of the person.

    Returns:
        str: The valid email address or an empty string if invalid.
    """
    email = (raw_email or '').strip()

    if not email:
        return None

    if email_is_valid(email):
        return email

    logger.warning("Invalid email '%s' for person %s", email, person_id)
    return None


def _extract_multi_emails(person_id: str, raw_emails: str | None) -> list[str]:
    """Extract multiple emails from a pipe-separated string."""
    emails = []
    for single_email in (raw_emails or '').split('|'):
        validated_email = validate_email_address(person_id, single_email)
        if validated_email:
            emails.append(validated_email)
    return emails


def extract_email_contact(
        entry: dict[str, str],
        person_id: str
) -> dict[str, str | None | list[str]]:
    """
    Extract and validate contact and personal emails.
    """
    if not (entry.get('email') or '').strip():
        logger.warning("Missing email for person %s", person_id)

    return {
        "email": validate_email_address(person_id, entry.get('email')),
        "other_email": _extract_multi_emails(person_id, entry.get('other_email')),
        "personal_email": _extract_multi_emails(person_id, entry.get('personal_email')),
    }


def _build_contacts(
        entry: dict[str, str],
        person_id: str
) -> list[dict[str, str | dict]]:
    emails_validated = extract_email_contact(entry, person_id)

    contacts = []

    contacts.append({
        'type': 'electronical_address',
        'format': 'email_address',
        'value': emails_validated.get('email'),
        'subtype': 'email',
    })

    for email_type in ['other_email', 'personal_email']:
        for email_value in emails_validated.get(email_type, []):
            contacts.append({
                'type': 'electronical_address',
                'format': 'email_address',
                'value': email_value,
                'subtype': email_type,
            })

    return contacts


@task(task_id="convert_spreadsheet_people")
def convert_spreadsheet_people(
        source_data: list[dict[str, str]],
        config: dict[str, str]
) -> dict[str, dict[
    str,
    list[
        dict[str,
        list[dict[str, str]]  # names: last_names / first_names
        ]
    ]
    | list[dict[str, str]]  # identifiers / memberships
    | list[dict[str, str | dict[str, str]]]  # contacts
    | list[dict[str, str | list[str] | None]]  # employments
]]:
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

        person_id = entry[LOCAL_PERSON_IDENTIFIER]

        non_empty_identifiers = extract_identifiers(entry)
        if not non_empty_identifiers:
            logger.warning("No identifiers for row: %s", entry)

        entity_uid = entry.get('main_research_structure', '').strip()

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
            'memberships': [{'entity_uid': entity_uid}] if entity_uid else [],
            'contacts': _build_contacts(entry, person_id),
        }

        employment = _build_employment(entry, config)
        if employment:
            result_entry['employments'] = [employment]

        task_results[entry[LOCAL_PERSON_IDENTIFIER]] = result_entry

    return task_results
