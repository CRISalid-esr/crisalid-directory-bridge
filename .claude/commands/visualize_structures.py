#!/usr/bin/env python3
"""
Generate an interactive HTML visualisation of a CRISalid structures CSV file.

Uses Cytoscape.js with a dagre hierarchical layout. Inclusion edges (solid)
define the hierarchy; participation edges (dashed, colour-coded by role) show
supervision relationships to external institutions.

Usage:
    python .claude/commands/visualize_structures.py <csv-path> [output-html-path]

If output-html-path is omitted the HTML is written next to the CSV with the same
basename and a .html extension (e.g. structures.csv → structures.html).
"""
import argparse
import csv
import json
import re
import sys
from pathlib import Path

POSITION_CODES = {'main_supervision', 'associated_supervision', 'participating_supervision'}

TEMPLATE    = Path(__file__).with_name('structures-visualization.html')
UAI_REF_CSV = Path(__file__).parent.parent.parent / 'data' / 'fr-esr-principaux-etablissements-enseignement-superieur.csv'


UAI_FALLBACK = {
    '0753639Y': 'CNRS',
    '0912423P': 'ENS Paris-Saclay',
}


def _load_uai_names() -> dict[str, str]:
    names = dict(UAI_FALLBACK)
    if not UAI_REF_CSV.exists():
        return names
    text = UAI_REF_CSV.read_text(encoding='utf-8-sig')
    names.update({
        row['uai - identifiant'].strip(): row['libellé'].strip()
        for row in csv.DictReader(text.splitlines(), delimiter=';')
        if row.get('uai - identifiant', '').strip()
    })
    return names


# ── Helpers ───────────────────────────────────────────────────────────────────

def _first_value(pipe_str: str) -> str:
    """Return the first non-empty pipe-separated value, stripped of [lang] suffix."""
    for part in pipe_str.split('|'):
        part = part.strip()
        if part:
            return re.sub(r'\[[^\]]+\]$', '', part).strip()
    return ''


def _strip_annotations(entry: str) -> str:
    """Remove all trailing [...] tokens to get the bare target UID."""
    return re.sub(r'(\[[^\]]*\])+$', '', entry).strip()


def _edge_label(entry: str) -> str:
    """Return the position code from the first [...] token, or empty string."""
    m = re.search(r'\[([^\]]+)\]', entry)
    if m and m.group(1) in POSITION_CODES:
        return m.group(1)
    return ''


def _split_pipe(value: str) -> list[str]:
    return [v.strip() for v in value.split('|') if v.strip()]


# ── Main ──────────────────────────────────────────────────────────────────────

def build_graph(csv_path: Path) -> dict:
    uai_names = _load_uai_names()

    text = csv_path.read_text(encoding='utf-8')
    reader = csv.DictReader(text.splitlines())
    rows = list(reader)

    nodes: list[dict] = []
    known_uids: set[str] = set()

    for row in rows:
        lid = row.get('local_id', '').strip()
        if not lid:
            continue
        # Rows marked generic_type=ignore are excluded from the graph
        if row.get('generic_type', '').strip() == 'ignore':
            continue
        uid = f'local-{lid}'
        known_uids.add(uid)
        nodes.append({
            'id':           uid,
            'local_id':     lid,
            'label':        _first_value(row.get('short_labels', '')),
            'long_label':   _first_value(row.get('long_labels', '')),
            'generic_type': row.get('generic_type', '').strip(),
            'national_type':row.get('type', '').strip(),
            'main_mission': row.get('main_mission', '').strip(),
            'group':        row.get('generic_type', '').strip() or 'external',
            'description':  _first_value(row.get('descriptions', '')),
            # identifiers for the info panel
            'nns':          row.get('nns', '').strip(),
            'ror':          row.get('ror', '').strip(),
            'uai':          row.get('uai', '').strip(),
            'isni':         row.get('isni', '').strip(),
            'wikidata':     row.get('wikidata', '').strip(),
            'scopus':       row.get('scopus', '').strip(),
        })

    edges: list[dict] = []
    ghost_ids: set[str] = set()

    for row in rows:
        lid = row.get('local_id', '').strip()
        if not lid:
            continue
        if row.get('generic_type', '').strip() == 'ignore':
            continue
        uid = f'local-{lid}'

        for entry in _split_pipe(row.get('inclusions', '')):
            target = _strip_annotations(entry)
            if not target:
                continue
            edges.append({'from': uid, 'to': target, 'dashes': False, 'label': ''})
            if target not in known_uids:
                ghost_ids.add(target)

        for entry in _split_pipe(row.get('participations', '')):
            target = _strip_annotations(entry)
            if not target:
                continue
            edges.append({'from': uid, 'to': target, 'dashes': True,
                          'label': _edge_label(entry)})
            if target not in known_uids:
                ghost_ids.add(target)

    for ghost in sorted(ghost_ids):
        if ghost.startswith('uai-'):
            ghost_label = uai_names.get(ghost[4:], ghost)
        else:
            ghost_label = ghost
        nodes.append({
            'id': ghost, 'local_id': ghost, 'label': ghost_label,
            'long_label': '', 'generic_type': '', 'national_type': '',
            'main_mission': '', 'group': 'external',
            'description': '', 'nns': '', 'ror': '', 'uai': '',
            'isni': '', 'wikidata': '', 'scopus': '',
        })

    # Isolated nodes: appear in no edge
    connected: set[str] = set()
    for e in edges:
        connected.add(e['from'])
        connected.add(e['to'])

    isolated = [n for n in nodes if n['id'] not in connected and n['group'] != 'external']
    isolated_ids = {n['id'] for n in isolated}
    nodes = [n for n in nodes if n['id'] not in isolated_ids]

    inclusion_edges     = sum(1 for e in edges if not e['dashes'])
    participation_edges = sum(1 for e in edges if e['dashes'])

    return {
        'filename': csv_path.name,
        'nodes':    nodes,
        'edges':    edges,
        'isolated': isolated,
        'root':     None,
        'stats': {
            'total':               len(rows),
            'inclusion_edges':     inclusion_edges,
            'participation_edges': participation_edges,
            'isolated_count':      len(isolated),
        },
    }


def generate(csv_path: str, output_path: str | None = None, root: str | None = None) -> None:
    src = Path(csv_path)
    dst = Path(output_path) if output_path else src.with_suffix('.html')

    data = build_graph(src)
    if root:
        data['root'] = f'local-{root}' if not root.startswith('local-') else root

    template = TEMPLATE.read_text(encoding='utf-8')
    html = template.replace(
        '/* __GRAPH_DATA__ */',
        f'const DATA = {json.dumps(data, ensure_ascii=False)};',
        1,
    )
    dst.write_text(html, encoding='utf-8')

    s = data['stats']
    print(
        f"Generated: {dst}  "
        f"({len(data['nodes'])} nodes, {s['inclusion_edges']} inclusion edges, "
        f"{s['participation_edges']} participation edges, {s['isolated_count']} isolated)"
    )


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate an interactive HTML structure visualisation.')
    parser.add_argument('csv_path', help='Path to the structures CSV file')
    parser.add_argument('output_path', nargs='?', help='Output HTML path (default: same dir as CSV)')
    parser.add_argument('--root-institution', metavar='LOCAL_ID',
                        help='local_id of the root institution (used for initial centering and 1-level expand)')
    args = parser.parse_args()
    generate(args.csv_path, args.output_path, args.root_institution)
