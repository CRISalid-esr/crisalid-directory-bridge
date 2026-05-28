Generate an interactive HTML visualisation of the structure hierarchy from a CSV file.

**Usage:** `/visualize-structures <csv-path> [output-html-path]`

If `output-html-path` is omitted, write the HTML file to the same directory as the CSV, with the same basename but a `.html` extension (e.g. `structures.csv` → `structures.html`).

---

## Step 1 — parse arguments

Split `$ARGUMENTS` on whitespace. First token = CSV path, second token (optional) = output path.
Derive the default output path from the CSV path if the second argument is absent.

---

## Step 2 — read and parse the CSV

Parse the CSV (UTF-8, comma-separated, first row = header). Collect all rows.

---

## Step 3 — build the node list

For every row produce a node object:

```json
{
  "id":           "local-{local_id}",
  "local_id":     "{local_id}",
  "label":        "{first short_label stripped of [lang] suffix}",
  "long_label":   "{first long_label stripped of [lang] suffix}",
  "generic_type": "{generic_type}",
  "national_type":"{type}",
  "main_mission": "{main_mission}",
  "group":        "{generic_type}",
  "tooltip":      ""
}
```

Collect all known UIDs into a set (`"local-{local_id}"` for every row).

---

## Step 4 — build the edge list

Helper — strip annotations from a relationship entry:
- Remove all trailing `[…]` tokens to get the bare target UID.
- From the first `[…]` token, if its content is a known position code (`main_supervision`, `associated_supervision`, `participating_supervision`), keep it as the edge label.

**Inclusion edges** (from `inclusions` column):
- Split on `|`, strip each entry.
- For every non-empty bare target: add edge `{ "from": "local-{local_id}", "to": "{target}", "dashes": false, "label": "" }`.

**Participation edges** (from `participations` column):
- Split on `|`, strip each entry.
- For every non-empty bare target: add edge `{ "from": "local-{local_id}", "to": "{target}", "dashes": true, "label": "{position_code_or_empty}" }`.

**Ghost nodes** — for every target referenced in any edge that is NOT in the known UIDs set, add a ghost node: `{ "id": "{target}", "local_id": "{target}", "label": "{target}", "group": "external" }`.

---

## Step 5 — detect isolated nodes

A node is isolated if its `id` does not appear in any edge's `from` or `to` field.

Build the `isolated` array from those nodes (same structure as the node objects above).

Remove isolated nodes from the main node list (they are rendered separately in the panel, not in the graph).

---

## Step 6 — assemble the DATA payload

```json
{
  "filename": "{basename of the CSV file}",
  "nodes":    [ … ],
  "edges":    [ … ],
  "isolated": [ … ],
  "stats": {
    "total":                {total rows},
    "inclusion_edges":      {count of non-dashed edges},
    "participation_edges":  {count of dashed edges},
    "isolated_count":       {len(isolated)}
  }
}
```

---

## Step 7 — read the template

Read `templates/structures-visualization.html` from the root of the `crisalid-directory-bridge` repository (the directory that contains `.claude/`).

---

## Step 8 — inject data and write output

In the template content, replace the exact string:

```
/* __GRAPH_DATA__ */
```

with:

```
const DATA = <JSON payload>;
```

Write the result to the output path.

---

## Step 9 — report

Print a single line:

```
Generated: <output-path>  (<N> nodes, <I> inclusion edges, <P> participation edges, <X> isolated)
```
