import logging
import csv

from datetime import datetime
from airflow.decorators import task

logger = logging.getLogger(__name__)

LOCAL_PERSON_IDENTIFIER = 'tracking_id'

PERSON_IDENTIFIERS = [LOCAL_PERSON_IDENTIFIER,
                      'eppn',
                      'idhal_i',
                      'idhal_s',
                      'orcid',
                      'idref',
                      'scopus_eid']

IDENTIFIER_TYPE_MAP = {
    'tracking_id': 'local',
    'idhal_i':'id_hal_i',
    'idhal_s':'id_hal_s',
}


def load_valid_positions(filepath: str) -> set[str]:
    """
    Load official HCERES position codes from a CSV file for validation.

    The CSV file must have a header row with a column named 'position_code'.
    Only the codes from that column will be considered valid values for the 'position' field
    in researcher records. Any non-matching value will cause a validation error.

    Args:
        filepath (str): Absolute or relative path to the HCERES nomenclature CSV file.

    Returns:
        set[str]: A set of official HCERES position codes.
    """
    with open(filepath, newline='', encoding='utf-8') as f:
        return {
            row['position_code'].strip()
            for row in csv.DictReader(f)
            if row.get('position_code', '').strip()
        }

VALID_POSITION_CODES = load_valid_positions('data/hceres_nomenclature.csv')


def is_valid_iso_date(date_str: str) -> bool:
    """Returns True if the date is in the YYYY-MM-DD format."""
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except ValueError:
        return False

@task(task_id="convert_spreadsheet_people")
def convert_spreadsheet_people(source_data: list[dict[str, str]]) -> dict[
    str, dict[str, str | dict]
]:
    """
    Extract the 'identifier' field from a dict of ldap entries

    Args:
        source_data (dict): A dict of ldap results with dn as key and entry as value

    Returns:
        dict: A dict of converted results with the "identifiers" field populated
    """


    task_results = {}



    for row in source_data:
        non_empty_identifiers = [
            {
                'type': IDENTIFIER_TYPE_MAP.get(identifier, identifier),
                'value': row[identifier]
            }
            for identifier in PERSON_IDENTIFIERS if row.get(identifier) and row[identifier].strip()
        ]

        if len(non_empty_identifiers) == 0:
            logger.warning("No identifiers for row: %s", row)

        entity_uid = row.get('main_research_structure', '').strip()

        employment = {}

        institution_identifier = row.get('institution_identifier', '').strip()
        if institution_identifier:
            employment['institution_identifier'] = institution_identifier

        #Strict HCERES nomenclature position validation

        position = row.get(
            'position', ''
        ).strip()

        if position:
            if position not in VALID_POSITION_CODES:
                person_id = row.get(LOCAL_PERSON_IDENTIFIER, '<unknown>')
                logger.error(
                    "Invalid position '%s' for person ID: %s. "
                    "Must match a code from hceres_nomenclature.csv.",
                    position, person_id
                )

                raise ValueError(
                    f"Invalid position '{position}' for person '{person_id}'. "
                    "Please update the input data or the HCERES nomenclature file."
                )
            employment['position'] = position

        # Strict UAI/ROR nomenclature institution validation

        institution_id_nomenclature = row.get(
            'institution_id_nomenclature', ''
        ).strip()

        if institution_id_nomenclature:
            if institution_id_nomenclature not in {'UAI', 'ROR'}:
                person_id = row.get(LOCAL_PERSON_IDENTIFIER, '<unknown>')
                logger.error(
                    "Invalid institution_id_nomenclature: '%s' (person ID: %s). "
                    "Must be 'UAI' or 'ROR'.",
                    institution_id_nomenclature, person_id
                )
                raise ValueError(
                    f"Invalid institution_id_nomenclature '{institution_id_nomenclature}' "
                    f"for person '{person_id}'. "
                    "Accepted values are 'UAI' or 'ROR'. Please fix the source file."
                )

            employment['institution_id_nomenclature'] = institution_id_nomenclature

        # Strict ISO 8601 date validation


        for date_key in ['employment_start_date', 'employment_departure_date']:
            date_value = row.get(
                date_key, ''
            ).strip()
            if date_value:
                if is_valid_iso_date(date_value):
                    employment[date_key] = date_value
                else:
                    person_id = row.get(LOCAL_PERSON_IDENTIFIER, '<unknown>')
                    logger.error(
                        "Invalid date format in field '%s': '%s' (person ID: %s). "
                        "Please fix the source file.",
                        date_key,
                        date_value,
                        person_id
                    )

                    raise ValueError(
                        f"Invalid date detected in field '{date_key}' for person '{person_id}': "
                        f"'{date_value}'. "
                        "Please correct the source file before re-running the task."
                    )

        task_results[f"{row[LOCAL_PERSON_IDENTIFIER]}"] = {
            'names': [
                {'last_names': [{'value': row['last_name'], 'language': 'fr'}],
                 'first_names': [
                     {'value': first_name, 'language': 'fr'}
                     for first_name in row.get('first_names', '').split(',')
                 ]}
            ],
            'identifiers': non_empty_identifiers,
            'memberships': [{'entity_uid': entity_uid}] if entity_uid else [],
            'employments': [employment] if employment else []
        }

    return task_results
