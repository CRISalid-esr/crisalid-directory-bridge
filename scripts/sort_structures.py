#!/usr/bin/env python3
"""
Reorder structure CSV rows to fix ordering violations.

Usage:
    python .claude/commands/sort_structures.py <csv-path>

Rewrites the file in-place with rows reordered so that every local-* reference
in inclusions and participations appears in an earlier row.

If a circular dependency is detected, prints a warning and exits without
modifying the file.
"""
import csv
import heapq
import re
import sys
from pathlib import Path


def _strip_annotations(entry: str) -> str:
    return re.sub(r'(\[[^\]]*\])+$', '', entry).strip()


def _local_deps(row: dict, known: set, self_lid: str) -> set:
    result = set()
    for col in ('inclusions', 'participations'):
        for entry in row.get(col, '').split('|'):
            target = _strip_annotations(entry.strip())
            if target.startswith('local-'):
                lid = target[len('local-'):]
                if lid in known and lid != self_lid:
                    result.add(lid)
    return result


def sort_csv(csv_path: str) -> int:
    path = Path(csv_path)
    try:
        text = path.read_text(encoding='utf-8')
    except OSError as exc:
        print(f"Cannot read file: {exc}", file=sys.stderr)
        return 1

    reader = csv.DictReader(text.splitlines())
    rows = list(reader)
    fieldnames = list(reader.fieldnames or [])

    # Rows without a local_id are preserved at the end, unchanged
    id_rows   = [(i, r) for i, r in enumerate(rows) if r.get('local_id', '').strip()]
    no_id_rows = [r for r in rows if not r.get('local_id', '').strip()]

    known      = {r['local_id'].strip() for _, r in id_rows}
    orig_pos   = {r['local_id'].strip(): idx for idx, (_, r) in enumerate(id_rows)}
    row_by_lid = {r['local_id'].strip(): r for _, r in id_rows}

    # deps[lid]  = set of lids that must appear before lid
    # rdeps[lid] = set of lids that depend on lid
    deps  = {lid: _local_deps(row_by_lid[lid], known, lid) for lid in known}
    rdeps: dict[str, set] = {lid: set() for lid in known}
    for lid, d in deps.items():
        for dep in d:
            rdeps[dep].add(lid)

    in_deg = {lid: len(d) for lid, d in deps.items()}

    # Stable Kahn's topological sort: min-heap keyed by original position
    heap: list = []
    for lid, deg in in_deg.items():
        if deg == 0:
            heapq.heappush(heap, (orig_pos[lid], lid))

    sorted_lids: list[str] = []
    while heap:
        _, lid = heapq.heappop(heap)
        sorted_lids.append(lid)
        for dependent in rdeps[lid]:
            in_deg[dependent] -= 1
            if in_deg[dependent] == 0:
                heapq.heappush(heap, (orig_pos[dependent], dependent))

    if len(sorted_lids) != len(known):
        cycle_nodes = sorted(lid for lid in known if lid not in set(sorted_lids))
        print("WARNING: circular dependency detected — file not modified")
        print(f"Nodes in cycle: {', '.join(cycle_nodes)}")
        return 1

    # Compare with original order to detect actual changes
    orig_order = [r['local_id'].strip() for _, r in id_rows]
    moved = [(orig, new) for orig, new in zip(orig_order, sorted_lids) if orig != new]

    if not moved:
        print(f"{path.name}: already in valid order — no changes made")
        return 0

    sorted_rows = [row_by_lid[lid] for lid in sorted_lids] + no_id_rows

    with path.open('w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(sorted_rows)

    print(f"{path.name}: reordered — {len(moved)} row(s) moved")
    return 0


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <csv-path>", file=sys.stderr)
        sys.exit(1)
    sys.exit(sort_csv(sys.argv[1]))
