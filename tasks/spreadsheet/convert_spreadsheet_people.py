import logging
import re
from datetime import datetime

import yaml
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
    'idhal_i': 'id_hal_i',
    'idhal_s': 'id_hal_s',
}

EMPLOYMENTS_DATE_MAP = {
    'employment_start_date': 'start_date',
    'employment_end_date': 'end_date'
}


def load_valid_positions_from_yml(filepath: str) -> set[str]:
    """
    Load official HCERES position codes from a YAML file for validation.

    The YAML file must contain a nested structure where each entry includes a 'corps' field.
    All 'corps' values will be considered valid codes.

    Args:
        filepath (str): Absolute or relative path to the HCERES YAML file.

    Returns:
        set[str]: A set of official HCERES position codes.
    """
    with open(filepath, encoding='utf-8') as f:
        data = yaml.safe_load(f)

    valid_codes = set()

    def recurse_extract_corps(node):
        if isinstance(node, list):
            for item in node:
                if isinstance(item, dict) and 'corps' in item:
                    valid_codes.add(item['corps'])
        elif isinstance(node, dict):
            for value in node.values():
                recurse_extract_corps(value)

    recurse_extract_corps(data)
    return valid_codes


VALID_POSITION_CODES = load_valid_positions_from_yml('conf/employee_types.yml')


def get_position_dict_from_yaml(code: str) -> tuple[str, str] | None:
    """
    Récupère (code, label) à partir du corps dans le YAML.
    """
    with open('conf/employee_types.yml', encoding='utf-8') as f:
        yaml_data = yaml.safe_load(f)

    for category in yaml_data.values():
        for subcategory in category.values():
            for item in subcategory:
                if item.get("corps") == code:
                    return (item["corps"], item.get("label", ""))

    return None


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

        entity_uid = None
        employment = {}

        institution_identifier = row.get('institution_identifier', '').strip()
        if institution_identifier:
            employment['institution_identifier'] = institution_identifier

        # Strict HCERES nomenclature position validation
        position_code = row.get(
            'position', ''
        ).strip()
        position_dict = None
        if position_code:
            if position_code not in VALID_POSITION_CODES:
                person_id = row.get(LOCAL_PERSON_IDENTIFIER, '<unknown>')
                logger.error(
                    "Invalid position '%s' for person ID: %s. "
                    "Must match a code from hceres nomenclature.",
                    position_code, person_id
                )

                raise ValueError(
                    f"Invalid position '{position_code}' for person '{person_id}'. "
                    "Please update the input data."
                )
            position_dict = get_position_dict_from_yaml(position_code)

        # Strict UAI/ROR nomenclature institution validation
        institution_id_nomenclature = row.get(
            'institution_id_nomenclature', ''
        ).strip()
        institution_id = row.get('institution_identifier', '').strip()
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

            if position_dict and institution_id_nomenclature in {'UAI', 'ROR'} and institution_id:
                entity_uid = f"{institution_id_nomenclature}-{institution_id}"
                employment = {
                    "position": {
                        "title": position_dict[1],
                        "code": position_dict[0]
                    },
                    "entity_uid": entity_uid
                }

        # Strict ISO 8601 date validation
        for key in ["employment_start_date", "employment_end_date"]:
            date_value = row.get(key, '').strip()
            if date_value:
                if is_valid_iso_date(date_value):
                    new_key = re.sub(r"^employment_", "", key)
                    employment[new_key] = date_value
                else:
                    person_id = row.get(LOCAL_PERSON_IDENTIFIER, '<unknown>')
                    logger.error(
                        "Invalid date format in field '%s': '%s' (person ID: %s). "
                        "Please fix the source file.",
                        key,
                        date_value,
                        person_id
                    )
                    raise ValueError(
                        f"Invalid date detected in field '{key}' "
                        f"for person '{person_id}': "
                        f"'{date_value}'. "
                        "Please correct the source file before re-running the task."
                    )

        entity_uid = row.get(
            'main_research_structure', ''
        ).strip()

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
