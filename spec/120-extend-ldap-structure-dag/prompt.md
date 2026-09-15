# Feature 120 — Extend LDAP structure DAG with CSV override

## Context

The `load_ldap_structures` DAG currently ingests structures exclusively from LDAP.
Some fields (e.g. `type`, `local_types`, `main_mission`, `secondary_missions`, `descriptions`,
external identifiers like `ror`, `nns`, `wikidata`) cannot be reliably derived from LDAP attributes
and must be provided manually.

The goal is to allow a CSV spreadsheet (same format as `load_spreadsheet_structure`) to **override
or complement** LDAP data, following the same dual-source pattern already in use for people
(`load_ldap_people.py` + `complete_identifiers`).

A companion script (`generate_structure_template.py`) will pre-fill the CSV from LDAP data so
operators only need to fill in the fields that LDAP cannot provide.

---

## 1. Script — `scripts/generate_structure_template.py`

### Purpose

Standalone CLI script (no Airflow dependency). Connects directly to LDAP using the same
`utils/ldap.py` helpers as the DAG (`connect_to_ldap`, `ldap_response_to_json_dict`) and writes a
CSV file with the same columns as the spreadsheet format consumed by `convert_spreadsheet_structures`.

Run from the repo root with credentials loaded from `.env.dev`:

```
APP_ENV=DEV python scripts/generate_structure_template.py --output data/structures_template.csv
```

`--output` defaults to stdout. No `--input` option: the script always fetches live from LDAP.

The output CSV is a **template**: LDAP-derivable fields are pre-filled; fields that require human
input are left empty.

### LDAP fetch

The script replicates the LDAP search logic from `fetch_structures_from_ldap.py` directly (no
shared utility): same `search_base` (`LDAP_STRUCTURES_BRANCH`), same `search_filter`
(`LDAP_STRUCTURES_FILTER`), same attributes list. This is intentional — the script is a one-off
tool and the duplication is acceptable.

### Runtime requirements

- Must be run from the repo root (so `utils/` is on the import path).
- Reads all LDAP connection/filter env vars (`LDAP_HOST`, `LDAP_PORT`, `LDAP_STRUCTURES_BRANCH`,
  `LDAP_STRUCTURES_FILTER`, `LDAP_STRUCTURE_GENERIC_TYPE`, `LDAP_DEFAULT_LANGUAGE`, …) from the
  environment — load via `APP_ENV=DEV` or export manually.
- No Airflow instance required.

### LDAP → CSV field mapping

| CSV column     | Source LDAP attribute             | Notes                                                          |
|----------------|-----------------------------------|----------------------------------------------------------------|
| `local_id`     | `supannCodeEntite`                | direct copy                                                    |
| `short_labels` | `ou` or `acronym`                 | first value; append `[{LDAP_DEFAULT_LANGUAGE}]`                |
| `long_labels`  | `eduOrgLegalName` then `ou`       | fallback chain; append `[{LDAP_DEFAULT_LANGUAGE}]`             |
| `descriptions` | `description`                     | first value; append `[{LDAP_DEFAULT_LANGUAGE}]`                |
| `web`          | `labeledURI`                      | first value; strip label part if present (format: `URI label`) |
| `city_adress`  | `postalAddress`                   | first value                                                    |
| `generic_type` | env `LDAP_STRUCTURE_GENERIC_TYPE` | default `unit`                                                 |
| `main_mission` | `businessCategory` via mapping    | `research/administration/library/pedagogy` → IKG value         |
| `inclusions`   | `supannCodeEntiteParent`          | format as `local-{parent_code}`                                |

Fields left **empty** in the template (require human input):
`type`, `local_types`, `secondary_missions`, `participations`,
`uai`, `nns`, `ror`, `isni`, `wikidata`, `scopus`,
`hceres_research_areas`, `erc_research_field`, `hal_collection`,
`signature`, `campus`

### Column order

Use the exact column order from `tasks/fetch_from_spreadsheet.py`
(`FETCH_PARAMETERS["spreadsheet_structures"]["columns"]`), plus any extra columns the existing
spreadsheet uses (`city_name`, `city_code`, `city_adress`).

### `businessCategory` → `main_mission` mapping

Same as `convert_ldap_structure_type.py`:

| `businessCategory` | `main_mission`            |
|--------------------|---------------------------|
| `research`         | `research`                |
| `administration`   | `administrative_services` |
| `library`          | `scientific_services`     |
| `pedagogy`         | `teaching`                |
| `organization`     | *(empty — not ingested)*  |

---

## 2. Test fixture — `tests/data/ldap/structures_ldap_response.py`

A Python module (not JSON) that defines a constant `LDAP_STRUCTURES_RESPONSE` — a dict in the
same format as the return value of `ldap_response_to_json_dict` (keyed by `supannCodeEntite`).
Using a Python module avoids JSON serialisation concerns and lets tests import it directly.

The fixture must cover:

- A unit with `businessCategory=research` and all standard attributes present
- A unit with `businessCategory=administration`
- A unit with no `businessCategory`
- A unit with `supannCodeEntiteParent` set (to test `inclusions` pre-fill)
- A unit with `eduOrgLegalName` absent (to test `ou` fallback for `long_labels`)

Use clearly fake codes (e.g. `TEST_U1`, `TEST_U2`, …). The fixture is shared between the script
test and any future task tests.

---

## 3. Test — `tests/scripts/test_generate_structure_template.py`

Unit-test the CSV-generation logic of `generate_structure_template.py` by mocking the LDAP layer.
Patch `utils.ldap.connect_to_ldap` and `utils.ldap.ldap_response_to_json_dict` to return
`LDAP_STRUCTURES_RESPONSE` from the fixture above, so no real LDAP connection is needed.

Extract the conversion logic into a pure function (e.g. `ldap_dict_to_csv_rows(ldap_data: dict,
lang: str, generic_type: str) -> list[dict]`) that the script calls after fetching. Tests call
this function directly with fixture data.

Test cases:

1. **Basic output shape** — output has all required columns; one row per LDAP entry.
2. **LDAP-derivable fields** — `local_id`, `short_labels`, `long_labels`, `descriptions`, `web`,
   `generic_type`, `main_mission`, `inclusions` are correctly pre-filled.
3. **Language tag** — labels/descriptions carry `[{lang}]` tag from `LDAP_DEFAULT_LANGUAGE`.
4. **Fallback chain** — when `eduOrgLegalName` absent, `long_labels` falls back to `ou`.
5. **Empty fields** — `type`, `ror`, `nns`, `wikidata`, etc. are empty strings.
6. **Parent ref** — when `supannCodeEntiteParent` is set, `inclusions` contains `local-{parent}`.
7. **No parent** — when `supannCodeEntiteParent` absent, `inclusions` is empty.
8. **`organization` businessCategory** — `main_mission` is empty (not mapped).

---

## 4. DAG update — `load_ldap_structures.py`

### New env var

`OVERRIDE_LDAP_STRUCTURE_DATA_FROM_SPREADSHEET` (boolean string, default `False`).

This supersedes the existing unused `COMPLETE_LDAP_STRUCTURE_IDENTIFIERS_FROM_SPREADSHEET` env
var. Remove the old var from `.env.example`, `.env.template`, `.env.test` and add the new one.

### DAG change

Mirror the pattern from `load_ldap_people.py`. After `combine_batch_results`, add a conditional
branch:

```python
combined_results = combine_batch_results(batch_results)

if get_env_variable("OVERRIDE_LDAP_STRUCTURE_DATA_FROM_SPREADSHEET"):
    spreadsheet_data = fetch_from_spreadsheet(entity_source, entity_type)
    final_results = override_ldap_structure_data(
        ldap_source=combined_results,
        spreadsheet_source=spreadsheet_data,
    )
else:
    final_results = combined_results

redis_keys = update_database(result=final_results, prefix=f"{entity_type}:{entity_source}:")
```

`entity_source` and `entity_type` are already defined in the DAG context (mirror values from
`load_spreadsheet_structure.py` for the structures entity type).

---

## 5. New task — `tasks/supann_2021/override_ldap_structure_data.py`

### Purpose

Merges LDAP-sourced combined records with spreadsheet-sourced records. The spreadsheet acts as an
override layer: it can enrich or correct any field.

### Merge rules

| Situation                           | Result                                                                                                                      |
|-------------------------------------|-----------------------------------------------------------------------------------------------------------------------------|
| Key in LDAP only                    | Keep LDAP record as-is                                                                                                      |
| Key in both LDAP and CSV            | Deep-merge: CSV fields take precedence over LDAP fields. Non-empty CSV values overwrite; empty CSV values do not overwrite. |
| Key in CSV only                     | Add the CSV record (new structure not in LDAP)                                                                              |
| CSV record has `generic_type` = `"ignore"`  | Drop the entry from the result entirely, even if it was present in LDAP. Log at INFO level.                           |

The merge key is `supannCodeEntite` on the LDAP side and `local_id` on the CSV side. A CSV
`local_id` with no matching LDAP key is expected and valid — the entry is simply added to the
output as a new structure (e.g. a purely administrative entity not present in LDAP). No warning
or error is raised for CSV-only entries.

### Signature

```python
@task(task_id="override_ldap_structure_data")
def override_ldap_structure_data(ldap_source: dict, spreadsheet_source: dict) -> dict:
    ...
```

Both arguments are dicts in the combined-record format output by `combine_batch_results` /
`convert_spreadsheet_structures`.

### Tests — `tests/tasks/supann_2021/test_override_ldap_structure_data.py`

Test cases:

1. **LDAP-only entry** — not in CSV → kept unchanged.
2. **CSV-only entry** — not in LDAP → added to output.
3. **Both, non-empty CSV field** — CSV value overwrites LDAP value.
4. **Both, empty CSV field** — LDAP value is preserved.
5. **Both, nested field (e.g. `identifiers`)** — merge is deep, not shallow replace.
6. **Empty inputs** — both empty dicts → empty output.
7. **`generic_type=ignore`** — CSV record with `generic_type="ignore"` causes the entry to be removed from the result whether it came from LDAP, CSV-only, or both.

---

## 6. Refactor — `utils/structure_utils.py`

### Motivation

`tasks/spreadsheet/convert_spreadsheet_structures.py` contains helpers (`_extract_label_value`,
`_extract_label_language`, `_parse_identifier_value`, `_parse_relationships`, …) that are useful
to both the spreadsheet converter and the new script. They belong in `utils/` — the existing
shared module layer — so that both `tasks/` and `scripts/` can import them without a circular or
architecturally awkward dependency.

### Target layout

```
utils/
└── structure_utils.py   # label, identifier, and relationship parsing helpers
```

Group all helpers in a single module (no sub-package needed at this scale).

### Steps

1. Move the private helpers from `convert_spreadsheet_structures.py` into `utils/structure_utils.py`.
2. Update imports in `convert_spreadsheet_structures.py` to use `utils.structure_utils`.
3. Import from `utils.structure_utils` in `generate_structure_template.py`.
4. Existing tests for `convert_spreadsheet_structures` must still pass without modification
   (no behavior change, only import paths move).

---

## 7. Env file updates

Add to `.env.example`, `.env.template`, and `.env.test`:

```
OVERRIDE_LDAP_STRUCTURE_DATA_FROM_SPREADSHEET=False
```

Remove `COMPLETE_LDAP_STRUCTURE_IDENTIFIERS_FROM_SPREADSHEET` from those same files if present.

---

## 8. Implementation order

1. Refactor into `utils/structure_utils.py` (step 6) — unblocks everything else
2. Write `tests/data/ldap/structures_ldap_response.py` fixture
3. Write `scripts/generate_structure_template.py` + tests
4. Write `tasks/supann_2021/override_ldap_structure_data.py` + tests
5. Update `load_ldap_structures.py` DAG
6. Update env files

Each step should be a separate commit.
