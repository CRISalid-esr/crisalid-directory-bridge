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
    Extract base identifier value, removing any date brackets.
    
    Dates are not stored in identifiers; they belong to relationships.
    
    Examples:
    - "E6[20190902-20251231]" -> 'E6'
    - "E42[20260101-]" -> 'E42'
    - "PATHO[1][20190902-20251231]" -> 'PATHO'
    - "SIMPLE_ID" -> 'SIMPLE_ID'
    """
    identifier_str = identifier_str.strip()
    
    # Remove all brackets and their content to get just the base value
    # Matches: ID[...][...] or ID[...]
    match = re.match(r'^(\w+)', identifier_str)
    return match.group(1) if match else identifier_str


def _extract_dates_from_identifier(identifier_str):
    """
    Extract date range from identifier string.
    
    Formats:
    - ID[startDate-endDate] -> {'start_date': 'startDate', 'end_date': 'endDate'}
    - ID[position][startDate-endDate] -> {'start_date': 'startDate', 'end_date': 'endDate'}
    - ID -> {}
    
    Returns: dict with start_date and/or end_date keys (or empty dict if no dates)
    """
    identifier_str = identifier_str.strip()
    
    # Try to match pattern with position and dates: ID[position][startDate-endDate]
    match = re.match(r'^(\w+)\[(\d+)\]\[(\d+)(?:-(\d*))?\]$', identifier_str)
    if match:
        return {
            'start_date': match.group(3),
            'end_date': match.group(4) if match.group(4) else None
        }
    
    # Try to match pattern with dates only: ID[startDate-endDate]
    match = re.match(r'^(\w+)\[(\d+)(?:-(\d*))?\]$', identifier_str)
    if match:
        return {
            'start_date': match.group(2),
            'end_date': match.group(3) if match.group(3) else None
        }
    
    # No dates found
    return {}


def _parse_relationships(inclusions_str, participations_str):
    """
    Parse relationship strings into proper format with dates on relationships.
    
    Formats:
    - inclusions: "TARGET_ID[startDate-endDate]" or "TARGET_ID" (is_part_of)
    - participations: "TARGET_ID[subtype]" or "TARGET_ID[subtype][startDate-endDate]" (member_of)
    """
    relationships = []
    
    # Parse inclusions: is_part_of relationships
    if inclusions_str and inclusions_str.strip():
        for inc in inclusions_str.split('|'):
            inc = inc.strip()
            if inc:
                target = _parse_identifier_value(inc)
                dates = _extract_dates_from_identifier(inc)
                rel = {
                    'type': 'is_part_of',
                    'target': target
                }
                rel.update(dates)  # Add start_date/end_date if present
                relationships.append(rel)
    
    # Parse participations: member_of relationships
    if participations_str and participations_str.strip():
        for part in participations_str.split('|'):
            part = part.strip()
            if part:
                # Format: TARGET[subtype] or TARGET[subtype][dates]
                # First extract the target and everything else
                target_match = re.match(r'^(\w+)', part)
                if not target_match:
                    continue
                    
                target = target_match.group(1)
                rel = {
                    'type': 'member_of',
                    'target': target
                }
                
                # Try to extract subtype: TARGET[subtype]...
                subtype_match = re.match(r'^\w+\[(\w+)\]', part)
                if subtype_match:
                    rel['subtype'] = subtype_match.group(1)
                
                # Try to extract dates if present
                dates = _extract_dates_from_identifier(part)
                rel.update(dates)  # Add start_date/end_date if present
                
                relationships.append(rel)
    
    return relationships


@task(task_id="convert_spreadsheet_structures")
def convert_spreadsheet_structures(source_data: list[dict[str, str]]) -> dict[str, dict[str, str | dict]]:
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
                    value = _parse_identifier_value(val.strip())
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
            secondary_missions = [m.strip() for m in str(row['secondary_missions']).split('|') if m.strip()]

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

