#!/usr/bin/env python3
"""
Validate a CRISalid structures CSV file.

This is the standalone Python equivalent of the /check-structures Claude Code command
(.claude/commands/check-structures.md). It implements the same checks (4a–4m) and
produces the same output format, so it can be run in CI or without Claude Code.

Usage:
    python .claude/commands/check_structures.py <csv-path>
"""
import csv
import re
import sys
import urllib.request
from pathlib import Path

# ── Constants ─────────────────────────────────────────────────────────────────

REQUIRED_COLUMNS = [
    'generic_type', 'type', 'local_types', 'main_mission', 'secondary_missions',
    'local_id', 'short_labels', 'long_labels', 'descriptions',
    'inclusions', 'participations',
    'uai', 'nns', 'ror', 'isni', 'wikidata', 'scopus',
    'erc_research_field', 'hceres_research_areas', 'hal_collection',
    'web', 'signature', 'campus',
]

VALID_GENERIC_TYPES = {
    'institution', 'institution_subdivision', 'unit', 'unit_subdivision', 'team',
}

VALID_MISSIONS = {
    'research', 'scientific_services', 'administrative_services', 'teaching',
}

KNOWN_EXTERNAL_PREFIXES = {'uai-', 'ror-', 'nns-', 'isni-', 'wikidata-', 'scopus-', 'local-'}

VALID_POSITION_CODES = {'main_supervision', 'associated_supervision', 'participating_supervision'}

FALLBACK_ALLOWED_NATIONAL_TYPES = {
    'institution':             {'UNIV', 'EPE', 'EPST', 'GE', 'COMUE'},
    'institution_subdivision': {'UFR', 'FAC', 'FDR'},
    'unit':                    {'UMR', 'UAR', 'UR', 'IRL'},
    'unit_subdivision':        set(),
    'team':                    {'TEAM', 'THEME'},
}

NATIONAL_TYPES_URL = (
    'https://raw.githubusercontent.com/CRISalid-esr/crisalid-ikg'
    '/refs/heads/dev-main/app/models/organization_types.py'
)


# ── Fetch national-type mapping ───────────────────────────────────────────────

def _fetch_allowed_national_types() -> dict[str, set[str]]:
    try:
        with urllib.request.urlopen(NATIONAL_TYPES_URL, timeout=5) as resp:
            source = resp.read().decode()
    except Exception:
        return FALLBACK_ALLOWED_NATIONAL_TYPES

    mapping: dict[str, set[str]] = {}
    # Parse blocks like: GenericOrganizationType.INSTITUTION: { NationalOrganizationType.EPE, ... }
    block_pattern = re.compile(
        r'GenericOrganizationType\.(\w+)\s*:\s*\{([^}]*)\}', re.DOTALL
    )
    value_pattern = re.compile(r'NationalOrganizationType\.(\w+)')
    for m in block_pattern.finditer(source):
        generic = m.group(1).lower()
        values = {v.group(1) for v in value_pattern.finditer(m.group(2))}
        mapping[generic] = values

    if not mapping:
        return FALLBACK_ALLOWED_NATIONAL_TYPES

    # Normalise enum key names that differ from string values
    key_map = {'institution_subdivision': 'institution_subdivision'}
    result = {}
    for raw_key, values in mapping.items():
        # GenericOrganizationType enum names are already lowercase snake_case
        result[raw_key] = values
    return result if result else FALLBACK_ALLOWED_NATIONAL_TYPES


# ── Helpers ───────────────────────────────────────────────────────────────────

def _strip_annotations(entry: str) -> str:
    """Remove all trailing [...] tokens from a relationship entry."""
    return re.sub(r'(\[[^\]]*\])+$', '', entry).strip()


def _bracket_tokens(entry: str) -> list[str]:
    """Return all [...] token contents from a relationship entry."""
    return re.findall(r'\[([^\]]*)\]', entry)


def _split_pipe(value: str) -> list[str]:
    return [v.strip() for v in value.split('|') if v.strip()]


# ── Issue collector ───────────────────────────────────────────────────────────

class _Issue:
    __slots__ = ('level', 'row', 'local_id', 'message')

    def __init__(self, level: str, row: int, local_id: str, message: str):
        self.level = level
        self.row = row
        self.local_id = local_id
        self.message = message


def _error(issues, row, local_id, msg):
    issues.append(_Issue('ERROR', row, local_id, msg))


def _warn(issues, row, local_id, msg):
    issues.append(_Issue('WARNING', row, local_id, msg))


# ── Main checker ──────────────────────────────────────────────────────────────

def check(csv_path: str) -> int:
    path = Path(csv_path)

    # Step 1 — load
    try:
        text = path.read_text(encoding='utf-8')
    except OSError as exc:
        print(f"Cannot read file: {exc}", file=sys.stderr)
        return 1

    try:
        reader = csv.DictReader(text.splitlines())
        rows = list(reader)
        header = reader.fieldnames or []
    except Exception as exc:
        print(f"Cannot parse CSV: {exc}", file=sys.stderr)
        return 1

    print(f"Checking: {path.name} ({len(rows)} rows)")

    # Step 2 — required columns
    missing_cols = [c for c in REQUIRED_COLUMNS if c not in header]
    if missing_cols:
        for col in missing_cols:
            print(f"  ERROR: missing required column '{col}'")
        print(f"\nResult: {len(missing_cols)} error(s) — cannot continue without required columns")
        return 1

    # Step 3 — build UID index (1-based; header = row 1, first data row = row 2)
    uid_row: dict[str, int] = {}
    seen_local_ids: dict[str, int] = {}  # local_id -> first row number
    for i, row in enumerate(rows, start=2):
        lid = row.get('local_id', '').strip()
        if lid:
            uid = f'local-{lid}'
            uid_row[uid] = i
            if lid in seen_local_ids:
                pass  # duplicates caught below
            else:
                seen_local_ids[lid] = i

    # Fetch national-type mapping
    allowed_national_types = _fetch_allowed_national_types()

    issues: list[_Issue] = []

    # Step 4 — per-row checks
    seen_ids_for_dup: dict[str, int] = {}

    for i, row in enumerate(rows, start=2):
        lid = row.get('local_id', '').strip()
        generic_type = row.get('generic_type', '').strip()

        # 4a — required fields
        if not lid:
            _error(issues, i, '', 'missing local_id')
            continue  # can't do further checks without an ID

        # generic_type=ignore marks a structure as intentionally excluded — skip field validation
        if generic_type == 'ignore':
            if lid in seen_ids_for_dup:
                _error(issues, i, lid,
                       f"duplicate local_id '{lid}' (also on row {seen_ids_for_dup[lid]})")
            else:
                seen_ids_for_dup[lid] = i
            continue

        if generic_type not in VALID_GENERIC_TYPES:
            _error(issues, i, lid, f"unknown generic_type '{generic_type}'")

        if not row.get('short_labels', '').strip():
            _error(issues, i, lid, 'missing short_label — at least one is required')

        if not row.get('long_labels', '').strip():
            _error(issues, i, lid, 'missing long_label — at least one is required')

        if not any(row.get(f, '').strip() for f in ('type', 'local_types', 'long_labels')):
            _error(issues, i, lid,
                   'no national_type, local_type, or long_label — at least one is required')

        # 4b — local_id format
        if len(lid) > 10 or not re.fullmatch(r'[\w:\-]+', lid):
            _error(issues, i, lid,
                   f"local_id '{lid}' is invalid — max 10 chars, "
                   f"allowed characters: alphanumeric, -, _, :")

        # 4c — duplicate local_id
        if lid in seen_ids_for_dup:
            _error(issues, i, lid,
                   f"duplicate local_id '{lid}' (also on row {seen_ids_for_dup[lid]})")
        else:
            seen_ids_for_dup[lid] = i

        # 4d — national_type compatibility
        national_type = row.get('type', '').strip()
        if national_type and generic_type in VALID_GENERIC_TYPES:
            allowed = allowed_national_types.get(generic_type, set())
            if national_type not in allowed:
                _warn(issues, i, lid,
                      f"national_type '{national_type}' is not in the expected list "
                      f"for generic_type '{generic_type}'")

        # 4e — mission fields
        main_mission = row.get('main_mission', '').strip()
        secondary_missions = row.get('secondary_missions', '').strip()
        if generic_type == 'unit':
            if main_mission not in VALID_MISSIONS:
                _error(issues, i, lid,
                       f"unit requires a valid main_mission; got '{main_mission}'")
        elif main_mission or secondary_missions:
            _warn(issues, i, lid,
                  f"mission fields (main_mission/secondary_missions) are only meaningful "
                  f"for units — '{generic_type}' should leave them empty")

        # 4f — conditional identifier requirements
        uai = row.get('uai', '').strip()
        nns = row.get('nns', '').strip()
        ror = row.get('ror', '').strip()
        if generic_type == 'institution':
            if not uai:
                _error(issues, i, lid, 'institutions require a UAI identifier')
        elif uai:
            _error(issues, i, lid,
                   f"UAI is only valid for institutions — remove it from this {generic_type}")

        if generic_type == 'unit' and main_mission == 'research':
            if not nns and not ror:
                _error(issues, i, lid, 'research units require at least one of: nns, ror')

        # 4g — identifier format
        if ror and re.match(r'https?://ror\.org/', ror):
            _error(issues, i, lid,
                   f"ror value must be the bare ID without URL prefix "
                   f"(e.g. '03xjwb504', not '{ror}')")

        isni = row.get('isni', '').strip()
        if isni and (' ' in isni or re.match(r'https?://isni\.org/', isni)):
            _error(issues, i, lid,
                   'isni value must be the bare ID without URL prefix or spaces')

        if nns and not re.fullmatch(r'\d{9}[A-Z]', nns):
            _warn(issues, i, lid,
                  f"nns value '{nns}' does not look like a valid RNSR number "
                  f"(expected 9 digits + 1 letter, e.g. 200412241T)")

        # 4h / 4i / 4j / 4k / 4l — relationship checks
        self_uid = f'local-{lid}'
        inclusions_str = row.get('inclusions', '').strip()
        participations_str = row.get('participations', '').strip()

        for col, col_str in (('inclusions', inclusions_str), ('participations', participations_str)):
            for entry in _split_pipe(col_str):
                target = _strip_annotations(entry)
                if not target:
                    continue

                # 4i — self-reference
                if target == self_uid:
                    _error(issues, i, lid,
                           f"self-reference in {col}: a structure cannot include itself")
                    continue

                # 4h — reference integrity + prefix
                if target.startswith('local-'):
                    if target not in uid_row:
                        _error(issues, i, lid,
                               f"broken reference in {col}: '{target}' not found in this file")
                    else:
                        # 4j — ordering
                        if uid_row[target] > i:
                            _error(issues, i, lid,
                                   f"ordering violation in {col}: '{target}' is defined on row "
                                   f"{uid_row[target]} which comes after this row — "
                                   f"move it earlier in the file")
                elif not any(target.startswith(p) for p in KNOWN_EXTERNAL_PREFIXES):
                    _warn(issues, i, lid,
                          f"unrecognised identifier prefix in {col}: '{target}'")

                # 4l — date annotation format
                for token in _bracket_tokens(entry):
                    if re.fullmatch(r'\d+', token) and len(token) != 8:
                        _warn(issues, i, lid,
                              f"suspicious date annotation '[{token}]' — "
                              f"expected YYYYMMDD (8 digits)")

                # 4n — position code in participations must use underscores
                if col == 'participations':
                    for token in _bracket_tokens(entry):
                        if re.fullmatch(r'\d+', token) or re.fullmatch(r'\d+.*', token):
                            continue  # date token, skip
                        if re.fullmatch(r'[a-zA-Z][\w-]*', token) and token not in VALID_POSITION_CODES:
                            _error(issues, i, lid,
                                   f"invalid position code '[{token}]' in participations — "
                                   f"must be one of: {', '.join(sorted(VALID_POSITION_CODES))}")

        # 4k — isolation
        if generic_type in ('unit_subdivision', 'team') and not inclusions_str:
            _warn(issues, i, lid,
                  f"isolated {generic_type} with no parent in inclusions")

        # 4m — label language tags
        for col in ('long_labels', 'short_labels', 'local_types', 'descriptions'):
            for entry in _split_pipe(row.get(col, '')):
                for tag in re.findall(r'\[([^\]]+)\]', entry):
                    if not re.fullmatch(r'[a-z]{2,3}', tag):
                        _warn(issues, i, lid,
                              f"suspicious tag '[{tag}]' in {col} — "
                              f"expected a language code like [fr] or [en]")

    # ── Output ─────────────────────────────────────────────────────────────────
    errors   = [x for x in issues if x.level == 'ERROR']
    warnings = [x for x in issues if x.level == 'WARNING']

    if not issues:
        print("✓ No issues found")
        return 0

    row_width = len(str(max(x.row for x in issues)))
    id_width  = max((len(x.local_id) for x in issues), default=0)

    def _fmt(issue: _Issue) -> str:
        return (
            f"  [row {issue.row:>{row_width}}] "
            f"local_id={issue.local_id:<{id_width}}  "
            f"{issue.message}"
        )

    if errors:
        print("\nERRORS")
        for e in errors:
            print(_fmt(e))

    if warnings:
        print("\nWARNINGS")
        for w in warnings:
            print(_fmt(w))

    print(f"\nResult: {len(errors)} error(s), {len(warnings)} warning(s)")
    return 1 if errors else 0


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <csv-path>", file=sys.stderr)
        sys.exit(1)
    sys.exit(check(sys.argv[1]))