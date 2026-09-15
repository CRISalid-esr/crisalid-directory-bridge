import re


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
                parsed = _parse_identifier_value(part)
                target = parsed['value']
                rel = {
                    'type': 'member_of',
                    'target': target
                }
                if 'subtype' in parsed:
                    rel['subtype'] = parsed['subtype']
                if 'start_date' in parsed:
                    rel['start_date'] = parsed['start_date']
                if 'end_date' in parsed:
                    rel['end_date'] = parsed['end_date']
                relationships.append(rel)

    return relationships
