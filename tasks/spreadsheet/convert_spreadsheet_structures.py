import logging

from airflow.sdk import task

from utils.structure_utils import (
    _extract_label_language,
    _extract_label_value,
    _parse_identifier_value,
    _parse_relationships,
)
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
