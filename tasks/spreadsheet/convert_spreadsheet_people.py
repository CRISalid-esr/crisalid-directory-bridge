import logging
from datetime import datetime
from typing import NoReturn
from airflow.decorators import task

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


def log_and_raise(person_id: str,
                  field: str,
                  value: str,
                  message: str) -> NoReturn:
    """
    Log an error and raise a ValueError with detailed context.

    Args:
        person_id (str): Identifier of the person involved in the error.
        field (str): The name of the field that caused the issue.
        value (str): The invalid value found.
        message (str): A human-readable explanation of the error.

    Raises:
        ValueError: Always raised after logging the error.
    """
    logger.error("%s (field: '%s', value: '%s', person ID: %s)",
                 message, field, value, person_id)
    raise ValueError(f"{message} (field '{field}', value '{value}', person '{person_id}')")


def validate_institution_nomenclature(nomenclature: str, person_id: str) -> str:
    """
    Validate the institution nomenclature value.

    The nomenclature must be either "UAI" or "ROR". If invalid, an error is logged
    and a ValueError is raised.

    Args:
        nomenclature (str): The institution nomenclature to validate.
        person_id (str): Identifier of the person linked to this record.

    Returns:
        str: The validated nomenclature (empty string is acceptable if no institution_id).

    Raises:
        ValueError: If the nomenclature is neither "UAI" nor "ROR".
    """
    if nomenclature and nomenclature not in {'UAI', 'ROR'}:
        log_and_raise(person_id, "institution_id_nomenclature", nomenclature,
                      "Invalid institution_id_nomenclature: must be 'UAI' or 'ROR'")
    return nomenclature


def add_employment_position(row_data: dict,
                            bodies_position_dict: dict[str, tuple[str, str]],
                            person_id: str) -> dict[str, str]:
    """
    Build a position dictionary based on the 'corps' field.

    If the 'corps' value exists and matches an entry in the YAML mapping,
    return a dict formatted for LDAP with title and code. Otherwise,
    return an empty dict.

    Args:
        row_data (dict): A single row from the spreadsheet data.
        bodies_position_dict (dict[str, tuple[str, str]]): Mapping of corps → (code, title).
        person_id (str): Identifier of the person.

    Returns:
        dict[str, str]: A position dictionary {"title": ..., "code": ...} or an empty dict.
    """
    position_data = row_data.get('position', '').strip()

    if not position_data:
        return {}

    if position_data in bodies_position_dict:
        code, title = bodies_position_dict[position_data]
        return {
            "title": title,
            "code": code
        }

    logger.warning(
        "Position '%s' not found in YAML mapping for person %s",
        position_data, person_id
    )
    return {}


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


def add_employment_dates(row_data: dict, person_id: str) -> dict[str, str]:
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
                log_and_raise(person_id, date_key, date_value,
                              "Invalid date format (expected YYYY-MM-DD)")
    return dates


def add_employment_hdr(row_data: dict, person_id: str) -> str:
    """
    Determine and normalize the HDR (Habilitation à Diriger des Recherches) value.

    Args:
        row_data (dict): The spreadsheet row containing HDR information.
        person_id (str): Identifier of the person.

    Returns:
        str: "oui" if HDR is confirmed, "non" otherwise (defaults to "non").

    Logs a warning if the HDR value is unrecognized and defaults to "non".
    """
    hdr_value = row_data.get('hdr', '').strip().lower()
    if hdr_value in ["oui", "non"]:
        return hdr_value

    if hdr_value == "":
        return "non"

    logger.warning(
        "Invalid HDR value '%s' for person %s. Defaulting to 'non'.",
        hdr_value,
        person_id
    )
    return "non"


def build_employment(row_data: dict[str, str],
                     bodies_position_dict: dict[str, tuple[str, str]]) -> dict[str, str | dict]:
    """
    Build and validate an employment structure for a person.

    This function constructs a standardized employment dictionary, validating
    required fields, checking dates, and attaching the position when applicable.

    Args:
        row_data (dict): The source spreadsheet row.
        bodies_position_dict (dict): Mapping of corps to positions.

    Returns:
        dict[str, str | dict]: A structured employment dictionary.
        Returns an empty dict if no institution_id is provided.

    Raises:
        ValueError: If mandatory fields are missing or invalid.
    """
    person_id = row_data.get(LOCAL_PERSON_IDENTIFIER)
    institution_id = row_data.get('institution_identifier', '').strip()
    institution_id_nomenclature = row_data.get('institution_id_nomenclature', '').strip()

    if not institution_id:
        return {}

    if not institution_id_nomenclature:
        log_and_raise(person_id, "institution_id_nomenclature", "<missing>",
                      "institution_id_nomenclature is required when institution_id is provided")

    validate_institution_nomenclature(institution_id_nomenclature, person_id)

    dates = add_employment_dates(row_data, person_id)
    position = add_employment_position(row_data, bodies_position_dict, person_id)

    employment: dict[str, str | dict] = {
        "entity_uid": f"UAI-{institution_id}",
        "hdr": add_employment_hdr(row_data, person_id)
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
        bodies_position_dict: dict[str, tuple[str, str]]
) -> dict[str, dict[str, str | dict]]:
    """
    Convert raw spreadsheet rows into structured person records.

    This Airflow task transforms spreadsheet data into a standardized structure
    with names, identifiers, memberships, and employment data.

    Args:
        source_data (list[dict[str, str]]): A list of spreadsheet rows, each row
            represented as a dictionary.
        bodies_position_dict (dict[str, tuple[str, str]]): Mapping of corps codes
            to (code, label) tuples.

    Returns:
        dict[str, dict[str, str | dict]]: A dictionary keyed by the LOCAL_PERSON_IDENTIFIER
        (e.g., tracking_id), where each value is a structured person record.
    """
    task_results = {}
    required_fields = [LOCAL_PERSON_IDENTIFIER, 'last_name', 'first_names']

    for row_data in source_data:
        for field in required_fields:
            if not row_data.get(field):
                raise ValueError(f"Missing required field '{field}' in row: {row_data}")

        non_empty_identifiers = extract_identifiers(row_data)
        if not non_empty_identifiers:
            logger.warning("No identifiers for row: %s", row_data)

        entity_uid = row_data.get('main_research_structure', '').strip()

        result_entry = {
            'names': [
                {
                    'last_names': [{'value': row_data['last_name'], 'language': 'fr'}],
                    'first_names': [
                        {'value': first_name, 'language': 'fr'}
                        for first_name in row_data.get('first_names', '').split(',')
                    ]
                }
            ],
            'identifiers': non_empty_identifiers,
            'memberships': [{'entity_uid': entity_uid}] if entity_uid else [],
        }

        employment = build_employment(row_data, bodies_position_dict)
        if employment:
            result_entry['employments'] = [employment]

        task_results[row_data[LOCAL_PERSON_IDENTIFIER]] = result_entry

    return task_results
