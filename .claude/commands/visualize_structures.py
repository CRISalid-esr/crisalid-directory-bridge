#!/usr/bin/env python3
"""
Generate an interactive HTML visualisation of a CRISalid structures CSV file.

This is the standalone Python equivalent of the /visualize-structures Claude Code command
(.claude/commands/visualize-structures.md). It implements the same data-building logic
and injects the result into the same HTML template, so it can be run without Claude Code.

Usage:
    python .claude/commands/visualize_structures.py <csv-path> [output-html-path]

If output-html-path is omitted the HTML is written next to the CSV with the same
basename and a .html extension (e.g. structures.csv → structures.html).
"""
import csv
import json
import re
import sys
from pathlib import Path

POSITION_CODES = {'main_supervision', 'associated_supervision', 'participating_supervision'}

TEMPLATE = Path(__file__).with_name('structures-visualization.html')


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
    text = csv_path.read_text(encoding='utf-8')
    reader = csv.DictReader(text.splitlines())
    rows = list(reader)

    nodes: list[dict] = []
    known_uids: set[str] = set()

    # Step 3 — build node list
    for row in rows:
        lid = row.get('local_id', '').strip()
        if not lid:
            continue
        uid = f'local-{lid}'
        known_uids.add(uid)
        nodes.append({
            'id':           uid,
            'local_id':     lid,
            'label':        _first_value(row.get('short_labels', '')),
            'long_label':   _first_value(row.get('long_labels', '')),
            'generic_type': row.get('generic_type', '').strip(),
            'national_type': row.get('type', '').strip(),
            'main_mission': row.get('main_mission', '').strip(),
            'group':        row.get('generic_type', '').strip(),
        })

    # Step 4 — build edge list + ghost nodes
    edges: list[dict] = []
    ghost_ids: set[str] = set()

    for row in rows:
        lid = row.get('local_id', '').strip()
        if not lid:
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
        nodes.append({'id': ghost, 'local_id': ghost, 'label': ghost, 'group': 'external'})

    # Step 5 — detect isolated nodes
    connected: set[str] = set()
    for e in edges:
        connected.add(e['from'])
        connected.add(e['to'])

    isolated = [n for n in nodes if n['id'] not in connected and n['group'] != 'external']
    isolated_ids = {n['id'] for n in isolated}
    nodes = [n for n in nodes if n['id'] not in isolated_ids]

    # Step 6 — assemble payload
    inclusion_edges    = sum(1 for e in edges if not e['dashes'])
    participation_edges = sum(1 for e in edges if e['dashes'])

    return {
        'filename': csv_path.name,
        'nodes':    nodes,
        'edges':    edges,
        'isolated': isolated,
        'stats': {
            'total':               len(rows),
            'inclusion_edges':     inclusion_edges,
            'participation_edges': participation_edges,
            'isolated_count':      len(isolated),
        },
    }


def generate(csv_path: str, output_path: str | None = None) -> None:
    src = Path(csv_path)
    dst = Path(output_path) if output_path else src.with_suffix('.html')

    data = build_graph(src)

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
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <csv-path> [output-html-path]", file=sys.stderr)
        sys.exit(1)
    generate(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else None)