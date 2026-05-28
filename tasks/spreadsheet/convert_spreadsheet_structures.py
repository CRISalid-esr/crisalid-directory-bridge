import logging
import re

from airflow.sdk import task

from utils.url_validators import is_valid_website_url

logger = logging.getLogger(__name__)

LOCAL_STRUCTURE_IDENTIFIER = 'local_id'

STRUCTURE_IDENTIFIERS = ['local_id', 'uai', 'nns', 'ror', 'isni', 'wikidata', 'scopus']

# Mapping of identifiers to their standardized type names
IDENTIFIER_TYPE_MAP = {
    'local_id': 'local'
}

# Research classifications (not identifiers)
STRUCTURE_RESEARCH_CLASSIFICATIONS = ['erc_research_field', 'hceres_research_areas']


def _extract_label_value(label_str):
    """Extract label value without language tag"""
    if not label_str:
        return None
    # Remove [language] suffix if present
    match = re.match(r'^(.+?)(?:\[[\w]+\])?$', label_str.strip())
    return match.group(1) if match else label_str.strip()


def _extract_label_language(label_str):
    """Extract language from [language] suffix, default to 'fr'"""
    if not label_str:
        return 'fr'
    match = re.search(r'\[(\w+)\]$', label_str.strip())
    return match.group(1) if match else 'fr'


def _parse_identifier_value(identifier_str):
    """
    Parse identifier string and extract value, position, and dates.
    
    Examples:
    - "PATHO_UNIT" -> {'value': 'PATHO_UNIT'}
    - "SU[]" -> {'value': 'SU'}  (empty brackets, no dates)
    - "SU[main_supervision]" -> {'value': 'SU', 'subtype': 'main_supervision'}
    - "uai-0780491K[associated_supervision][20210101]" ->
        {'value': 'uai-0780491K', 'subtype': 'associated_supervision', 'start_date': '20210101', 'end_date': None}
    - "E6[20190902-20251231]" -> {'value': 'E6', 'start_date': '20190902', 'end_date': '20251231'}
    - "E42[20260101-]" -> {'value': 'E42', 'start_date': '20260101', 'end_date': None}
    - "PATHO[1][20190902-20251231]" ->
        {'value': 'PATHO', 'position': '1', 'start_date': '20190902', 'end_date': '20251231'}
    - "PATHO[2][20260101-]" ->
        {'value': 'PATHO', 'position': '2', 'start_date': '20260101', 'end_date': None}
    
    Returns: dict with 'value' key, and optionally 'position', 'start_date', 'end_date'
    """
    identifier_str = identifier_str.strip()
    result = {}

    # ID[text_subtype][startDate-endDate]  e.g. uai-0780491K[associated_supervision][20210101]
    match = re.match(r'^([\w-]+)\[([a-zA-Z_]+)\]\[(\d+)(?:-(\d*))?\]$', identifier_str)
    if match:
        result['value'] = match.group(1)
        result['subtype'] = match.group(2)
        result['start_date'] = match.group(3)
        result['end_date'] = match.group(4) if match.group(4) else None
        return result

    # ID[text_subtype]  e.g. uai-0753639Y[main_supervision]
    match = re.match(r'^([\w-]+)\[([a-zA-Z_]+)\]$', identifier_str)
    if match:
        result['value'] = match.group(1)
        result['subtype'] = match.group(2)
        return result

    # Try to match pattern with position and dates: ID[position][startDate-endDate]
    match = re.match(r'^([\w-]+)\[(\d+)\]\[(\d+)(?:-(\d*))?\]$', identifier_str)
    if match:
        result['value'] = match.group(1)
        result['position'] = match.group(2)
        result['start_date'] = match.group(3)
        result['end_date'] = match.group(4) if match.group(4) else None
        return result

    # Try to match pattern with dates only: ID[startDate-endDate]
    match = re.match(r'^([\w-]+)\[(\d+)(?:-(\d*))?\]$', identifier_str)
    if match:
        result['value'] = match.group(1)
        result['start_date'] = match.group(2)
        result['end_date'] = match.group(3) if match.group(3) else None
        return result

    # Try to match pattern with empty brackets or subtype: ID[] or ID[subtype]
    match = re.match(r'^([\w-]+)\[\w*\]$', identifier_str)
    if match:
        result['value'] = match.group(1)
        return result

    # No dates or position found, just the identifier
    result['value'] = identifier_str
    return result


def _parse_relationships(inclusions_str, participations_str):
    """
    Parse relationship strings into proper format with dates on relationships.
    
    Formats:
    - inclusions: "TARGET_ID[startDate-endDate]" or "TARGET_ID" (part_of)
    - participations: "TARGET_ID[subtype]" or "TARGET_ID[subtype][startDate-endDate]" (member_of)
    """
    relationships = []

    # Parse inclusions: part_of relationships
    if inclusions_str and inclusions_str.strip():
        for inc in inclusions_str.split('|'):
            inc = inc.strip()
            if inc:
                parsed = _parse_identifier_value(inc)
                rel = {
                    'type': 'part_of',
                    'target': parsed['value']
                }
                # Add dates if present
                if 'start_date' in parsed:
                    rel['start_date'] = parsed['start_date']
                if 'end_date' in parsed:
                    rel['end_date'] = parsed['end_date']
                relationships.append(rel)

    # Parse participations: member_of relationships
    if participations_str and participations_str.strip():
        for part in participations_str.split('|'):
            part = part.strip()
            if part:
                # Parse the full identifier with all components
                parsed = _parse_identifier_value(part)
                target = parsed['value']
                rel = {
                    'type': 'member_of',
                    'target': target
                }

                if 'subtype' in parsed:
                    rel['subtype'] = parsed['subtype']

                # Add dates if present
                if 'start_date' in parsed:
                    rel['start_date'] = parsed['start_date']
                if 'end_date' in parsed:
                    rel['end_date'] = parsed['end_date']

                relationships.append(rel)

    return relationships


@task(task_id="convert_spreadsheet_structures")
def convert_spreadsheet_structures(source_data: list[dict[str, str]]) -> dict[
    str, dict[str, str | dict]]:
    """
    Convert spreadsheet structure data to the standard output format

    Args:
        source_data (list): List of structure records from CSV

    Returns:
        dict: A dict of converted results with the local_id as key and standard format as value
    """

    task_results = {}

    for row in source_data:
        local_id = row.get(LOCAL_STRUCTURE_IDENTIFIER) or row.get('tracking_id')
        if not local_id:
            logger.warning("Skipping row without local_id or tracking_id")
            continue

        # Parse labels
        short_labels = []
        if row.get('short_labels'):
            for label in str(row['short_labels']).split('|'):
                label = label.strip()
                if label:
                    short_labels.append({
                        'value': _extract_label_value(label),
                        'language': _extract_label_language(label)
                    })

        long_labels = []
        if row.get('long_labels'):
            for label in str(row['long_labels']).split('|'):
                label = label.strip()
                if label:
                    long_labels.append({
                        'value': _extract_label_value(label),
                        'language': _extract_label_language(label)
                    })

        descriptions = []
        if row.get('descriptions'):
            for desc in str(row['descriptions']).split('|'):
                desc = desc.strip()
                if desc:
                    descriptions.append({
                        'value': _extract_label_value(desc),
                        'language': _extract_label_language(desc)
                    })

        # Build identifiers
        non_empty_identifiers = []
        for identifier in STRUCTURE_IDENTIFIERS:
            if row.get(identifier) and str(row[identifier]).strip():
                identifier_values = str(row[identifier]).split('|')
                for val in identifier_values:
                    # Extract just the base identifier value (no dates)
                    parsed = _parse_identifier_value(val.strip())
                    value = parsed.get('value') if isinstance(parsed, dict) else parsed
                    if value:  # Only add non-empty values
                        identifier_obj = {
                            'type': IDENTIFIER_TYPE_MAP.get(identifier, identifier),
                            'value': value
                        }
                        non_empty_identifiers.append(identifier_obj)

        # Build contacts
        contacts = []
        web_address = row.get('web', '').strip()
        if web_address:
            if is_valid_website_url(web_address):
                contacts.append({
                    'type': 'electronical_address',
                    'format': 'website_address',
                    'value': {'uri': web_address}
                })
            else:
                logger.warning(
                    "Website address failed validation: %r (entry local_id=%s). "
                    "Expected format: http(s)://...",
                    web_address,
                    local_id
                )

        # Parse relationships
        relationships = _parse_relationships(
            row.get('inclusions', ''),
            row.get('participations', '')
        )

        # Parse secondary_missions
        secondary_missions = []
        if row.get('secondary_missions'):
            secondary_missions = [m.strip() for m in str(row['secondary_missions']).split('|') if
                                  m.strip()]

        # Parse local_types
        local_types = []
        if row.get('local_types'):
            local_types = [t.strip() for t in str(row['local_types']).split('|') if t.strip()]

        # Parse research classifications
        research_classifications = {}
        for classification_type in STRUCTURE_RESEARCH_CLASSIFICATIONS:
            if row.get(classification_type):
                values = [v.strip() for v in str(row[classification_type]).split('|') if v.strip()]
                if values:
                    research_classifications[classification_type] = values

        task_results[local_id] = {
            'generic_type': row.get('generic_type', 'unit'),
            'type': row.get('type') or None,
            'local_types': local_types,
            'main_mission': row.get('main_mission', 'research'),
            'secondary_missions': secondary_missions,
            'long_labels': long_labels,
            'short_labels': short_labels,
            'descriptions': descriptions,
            'identifiers': non_empty_identifiers,
            'relationships': relationships,
            'contacts': contacts
        }

        # Add research classifications if present
        if research_classifications:
            task_results[local_id]['research_classifications'] = research_classifications

    return task_results
