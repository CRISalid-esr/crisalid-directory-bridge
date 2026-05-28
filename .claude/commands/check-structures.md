Validate the structures CSV file at `$ARGUMENTS`.

Read the file, parse every row, and report all issues. For each issue print the row number, the `local_id`, and a clear plain-language description. At the end print a summary line.

---

## Step 1 — load the file

Parse `$ARGUMENTS` as a CSV (first row = header). Collect all rows into a list. Note the filename and row count for the header line.

If the file cannot be read or parsed, stop and report that immediately.

---

## Step 2 — check required columns

The following columns must be present in the header (case-sensitive). This list is the authoritative one from `tasks/fetch_from_spreadsheet.py` (`FETCH_PARAMETERS["spreadsheet_structures"]["columns"]`):

`generic_type`, `type`, `local_types`, `main_mission`, `secondary_missions`, `local_id`,
`short_labels`, `long_labels`, `descriptions`, `inclusions`, `participations`,
`uai`, `nns`, `ror`, `isni`, `wikidata`, `scopus`,
`erc_research_field`, `hceres_research_areas`, `hal_collection`, `web`, `signature`, `campus`

Report each missing column as an **ERROR** and stop further checks if any are missing.

Extra columns not in this list (e.g. `city_name`, `city_code`, `city_adress`) are allowed and silently ignored.

---

## Step 3 — build the known-UID index

For every row, compute `local-{local_id}` and store its row number (1-based, header = row 1, first data row = row 2) in a dict called **uid_row**:

```
uid_row: dict[str, int]  # e.g. {"local-U_9015": 8, "local-U_9015_A": 9, …}
```

This single index is used for both reference-integrity checks (does the target exist?) and ordering checks (does it appear before the referencing row?).

---

## Step 4 — per-row checks (run for every row)

### 4a — required fields

- `local_id` must be non-empty → ERROR: "missing local_id"
- `generic_type` must be one of `institution`, `institution_subdivision`, `unit`, `unit_subdivision`, `team` → ERROR: "unknown generic_type '{value}'"
- At least one of `type`, `local_types`, `long_labels` must be non-empty → ERROR: "no national_type, local_type, or long_label — at least one is required"
- `short_labels` must be non-empty → ERROR: "missing short_label — at least one is required"
- `long_labels` must be non-empty → ERROR: "missing long_label — at least one is required"

### 4b — local_id format

`local_id` must be at most 10 characters and contain only alphanumeric characters, hyphens (`-`), underscores (`_`), or colons (`:`) → ERROR: "local_id '{value}' is invalid — max 10 chars, allowed characters: alphanumeric, -, _, :"

### 4c — duplicate local_id

If the same `local_id` appears more than once → ERROR on each duplicate row: "duplicate local_id '{id}' (also on row N)"

### 4d — national_type compatibility

If `type` is non-empty, verify it is allowed for the row's `generic_type`.

Fetch the authoritative mapping from:
https://raw.githubusercontent.com/CRISalid-esr/crisalid-ikg/refs/heads/dev-main/app/models/organization_types.py

Parse `ALLOWED_NATIONAL_TYPES_BY_GENERIC_TYPE` from that file to get the current allowed sets. If the URL is unreachable, fall back to the last known values:

| generic_type | allowed type values |
|---|---|
| `institution` | UNIV, EPE, EPST, GE, COMUE |
| `institution_subdivision` | UFR, FAC, FDR |
| `unit` | UMR, UAR, UR, IRL |
| `unit_subdivision` | *(none)* |
| `team` | TEAM, THEME |

→ WARNING: "national_type '{type}' is not in the expected list for generic_type '{generic_type}'"

### 4e — mission fields

If `generic_type` is `unit`: `main_mission` must be one of `research`, `scientific_services`, `administrative_services`, `teaching` → ERROR: "unit requires a valid main_mission; got '{value}'"

If `generic_type` is anything other than `unit` and `main_mission` or `secondary_missions` is non-empty → WARNING: "mission fields (main_mission/secondary_missions) are only meaningful for units — '{generic_type}' should leave them empty"

### 4f — conditional identifier requirements

- If `generic_type` is `institution`: `uai` must be non-empty → ERROR: "institutions require a UAI identifier"
- If `generic_type` is not `institution` and `uai` is non-empty → ERROR: "UAI is only valid for institutions — remove it from this {generic_type}"
- If `generic_type` is `unit` and `main_mission` is `research`: at least one of `nns` or `ror` must be non-empty → ERROR: "research units require at least one of: nns, ror"

### 4g — identifier format

- `ror`: must not start with `https://ror.org/` or `http://ror.org/` → ERROR: "ror value must be the bare ID without URL prefix (e.g. '03xjwb504', not 'https://ror.org/03xjwb504')"
- `isni`: must not contain spaces or start with `https://isni.org/` → ERROR: "isni value must be the bare ID without URL prefix or spaces"
- `nns`: must match the RNSR pattern — 9 digits followed by one uppercase letter (e.g. `200412241T`) → WARNING: "nns value '{value}' does not look like a valid RNSR number (expected 9 digits + 1 letter, e.g. 200412241T)"

### 4h — reference integrity

For both `inclusions` and `participations`:
- Split on `|`
- For each entry, strip any trailing `[...]` annotations (e.g., `[main_supervision]`, `[20210101]`, `[20210101-20231231]`) to get the bare target UID
- If the bare target starts with `local-`: check it exists in **uid_row** → ERROR: "broken reference in {column}: '{target}' not found in this file"
- If the bare target does not start with a recognised external prefix (`uai-`, `ror-`, `nns-`, `isni-`, `wikidata-`, `scopus-`, `local-`): WARNING "unrecognised identifier prefix in {column}: '{target}'"

### 4i — self-reference

If any target in `inclusions` or `participations` (after stripping annotations) equals `local-{local_id}` of the same row → ERROR: "self-reference in {column}: a structure cannot include itself"

### 4j — ordering

Messages are generated and sent sequentially in row order. Any `local-*` target referenced in `inclusions` or `participations` must appear in an **earlier row** than the current one, otherwise it will not yet exist in the graph when this row's message is processed.

For each `local-*` target (after stripping annotations): look up its row number in **uid_row**. If that number is greater than the current row → ERROR: "ordering violation in {column}: '{target}' is defined on row N which comes after this row — move it earlier in the file"

### 4k — isolation

If `generic_type` is `unit_subdivision` or `team` and `inclusions` is empty → WARNING: "isolated {generic_type} with no parent in inclusions"

### 4l — date annotations

In `inclusions` and `participations`, find all `[...]` tokens. Any token that looks like a date (contains only digits) but is not exactly 8 digits → WARNING: "suspicious date annotation '{token}' — expected YYYYMMDD (8 digits)"

### 4m — label language tags

In `long_labels`, `short_labels`, `local_types`, `descriptions`, for each `|`-separated entry find any `[...]` suffix. If the content is not 2–3 lowercase letters → WARNING: "suspicious tag '[{content}]' in {column} — expected a language code like [fr] or [en]"

---

## Step 5 — output format

Print:

```
Checking: <filename> (<N> rows)
```

Then two sections (omit a section entirely if it is empty):

```
ERRORS
  [row  3] local_id=U_9015_A   broken reference in inclusions: 'local-U_9015_X' not found in this file
  ...

WARNINGS
  [row  8] local_id=U_9015     unrecognised identifier prefix in participations: 'nns-202123711L'
  ...
```

Align columns for readability. Use the actual 1-based row number (header = row 1, first data row = row 2).

End with:

```
Result: <E> error(s), <W> warning(s)
```

or, if everything is clean:

```
✓ No issues found
```