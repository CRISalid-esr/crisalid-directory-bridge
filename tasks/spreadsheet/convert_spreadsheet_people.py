import logging
import re
from datetime import datetime

from airflow.decorators import task
from utils.yaml_loader import get_position_by_code

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



def validate_required_fields(person_data: dict[str, str]) -> None:
    """
    Ensure mandatory fields are present and non-empty.
    Raises ValueError if a required field is missing.
    """
    for field_name in ["tracking_id", "first_names", "last_name"]:
        if not person_data.get(field_name, "").strip():
            raise ValueError(f"Missing mandatory field '{field_name}' in person record.")


def validate_institution_logic(person_data: dict[str, str]) -> None:
    """
    Validate consistency for institution-related fields.

    Rules:
    - If 'institution_identifier' is provided, 'institution_id_nomenclature' becomes mandatory.
    - Fields 'position', 'employment_start_date' and 'employment_end_date' are only meaningful
      if 'institution_identifier' is provided.
    """
    institution_identifier = person_data.get("institution_identifier", "").strip()
    institution_nomenclature = person_data.get("institution_id_nomenclature", "").strip()

    if institution_identifier and not institution_nomenclature:
        raise ValueError(
            f"'institution_id_nomenclature' is required when 'institution_identifier' is provided "
            f"(record for tracking_id '{person_data.get(LOCAL_PERSON_IDENTIFIER)}')."
        )



def extract_identifiers(person_data: dict[str, str]) -> tuple[str, list[dict[str, str]]]:
    """
    Extract identifiers from the person record
    and return both the tracking_id and the list of identifiers.
    Raises ValueError if tracking_id is missing (handled by validate_required_fields).
    """
    tracking_id = person_data[LOCAL_PERSON_IDENTIFIER].strip()
    identifiers = [
        {'type': IDENTIFIER_TYPE_MAP.get(identifier, identifier), 'value': person_data[identifier]}
        for identifier in PERSON_IDENTIFIERS
        if person_data.get(identifier) and person_data[identifier].strip()
    ]
    return tracking_id, identifiers


def build_position(person_data: dict[str, str]) -> tuple[str, str] | None:
    """
    Return (code, label) from YAML if position_code exists, else None.
    Chain original ValueError from YAML for more context.
    """
    position_code = person_data.get('position', '').strip()
    if not position_code:
        return None

    try:
        return get_position_by_code(position_code)
    except ValueError as exc:
        tracking_id = person_data[LOCAL_PERSON_IDENTIFIER].strip()
        raise ValueError(
            f"Invalid position '{position_code}' for person with tracking_id '{tracking_id}'. "
            "Must match a code from YAML nomenclature."
        ) from exc


def build_institution(person_data: dict[str, str], position: tuple[str, str] | None) -> dict:
    """
    Build the employment dictionary based on institution info and position.
    Returns an empty dict if institution_identifier is not provided.
    """
    institution_identifier = person_data.get('institution_identifier', '').strip()
    institution_nomenclature = person_data.get('institution_id_nomenclature', '').strip()

    if not institution_identifier:
        return {}  # nothing to build if there is no institution

    employment_entry: dict[str, str | dict] = {
    }

    if position:
        employment_entry["position"] = {"title": position[1], "code": position[0]}
        employment_entry["entity_uid"] = f"{institution_nomenclature}-{institution_identifier}"

    return employment_entry


def add_dates_to_employment(person_data: dict[str, str], employment_entry: dict) -> None:
    """
    Validate ISO dates and add them to the employment dict if valid.
    The check is done inline instead of using a separate helper.
    """
    for key in ["employment_start_date", "employment_end_date"]:
        date_value = person_data.get(key, '').strip()
        if not date_value:
            continue

        try:
            datetime.strptime(date_value, '%Y-%m-%d')
        except ValueError as exc:
            tracking_id = person_data[LOCAL_PERSON_IDENTIFIER].strip()
            raise ValueError(
                f"Invalid date format in field '{key}' for person '{tracking_id}': '{date_value}'. "
                "Expected format is YYYY-MM-DD. Please correct the source file."
            ) from exc

        new_key = re.sub(r"^employment_", "", key)
        employment_entry[new_key] = date_value



@task(task_id="convert_spreadsheet_people")
def convert_spreadsheet_people(source_data:
list[dict[str, str]]) -> dict[str, dict[str, str | dict]]:
    """
    Convert spreadsheet people data into a normalized structure with identifiers,
    employment info, and memberships.

    - Validates mandatory fields and institution rules.
    - Extracts and structures names, identifiers, memberships, and employments.
    """
    converted_people: dict[str, dict[str, str | dict]] = {}

    for person_data in source_data:

        validate_required_fields(person_data)
        validate_institution_logic(person_data)

        tracking_id, identifiers = extract_identifiers(person_data)
        if not identifiers:
            logger.warning("No identifiers for person with tracking_id: %s", tracking_id)

        position_entry = build_position(person_data)
        employment_entry = build_institution(person_data, position_entry)
        add_dates_to_employment(person_data, employment_entry)

        main_structure = person_data.get('main_research_structure', '').strip()

        converted_people[tracking_id] = {
            'names': [
                {
                    'last_names': [{'value': person_data['last_name'], 'language': 'fr'}],
                    'first_names': [
                        {'value': first_name, 'language': 'fr'}
                        for first_name in person_data.get('first_names', '').split(',')
                    ]
                }
            ],
            'identifiers': identifiers,
            'memberships': [{'entity_uid': main_structure}]
            if main_structure else [],
            'employments': [employment_entry] if employment_entry else []
        }

    return converted_people
