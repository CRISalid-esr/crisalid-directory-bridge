# CLAUDE.md

## Git commits

- Commit messages must be short and to the point. Short bullet list, no detailed explanations.
- Do not add a `Co-Authored-By` trailer or any other signature to commits.
- Never commit `.env.dev` — it contains local secrets and is gitignored.
- Always update `.env.example`, `.env.template`, and `.env.test` when adding new environment variables.

## Running tests

```bash
AIRFLOW_HOME=~/airflow /home/joachim/code/cdb-venv311/bin/python -m pytest <test files> -v
```

The virtualenv is at `/home/joachim/code/cdb-venv311`. Always use it — the system Python does not have the project dependencies.

## Application architecture

**CRISalid directory bridge** is a set of Apache Airflow DAGs that watch institutional data sources (LDAP directory, spreadsheets, HR databases) and convert their data into a common format that is broadcast to the CRISalid knowledge graph (IKG) and other components via RabbitMQ.

### What it does

1. **Fetch** — pull raw data from a source (LDAP, spreadsheet, PostgreSQL, …)
2. **Convert** — parallel field-level tasks transform raw source data into IKG-compliant fields
3. **Combine** — `combine_batch_results` merges all field dicts into one record per entity
4. **Store** — `update_database` writes versioned records to Redis
5. **Broadcast** — `broadcast_entities` DAG (triggered downstream) reads from Redis and publishes AMQP messages to RabbitMQ

### DAG overview

| DAG | Source | Entity |
|---|---|---|
| `load_ldap_structures` | LDAP (SUPANN 2021) | Structures |
| `load_ldap_people` | LDAP (SUPANN 2021) | People |
| `load_spreadsheet_structure` | CSV spreadsheet | Structures |
| `load_spreadsheet_people` | CSV spreadsheet | People |

### Task layout

Each `load_*` DAG follows the same pattern:

```
fetch_*_from_ldap / fetch_from_spreadsheet
        │  (parallel)
        ├─ convert_*_field_1
        ├─ convert_*_field_2
        └─ convert_*_field_N
        │
        ▼
combine_batch_results   ← merges all field dicts by entity key
        │
        ▼
update_database         ← stores versioned records in Redis
        │
        ▼
trigger_broadcast       ← fires broadcast_entities DAG
```

Conversion tasks are loaded dynamically from environment variables (e.g. `LDAP_STRUCTURE_LONG_LABELS_TASK`). This allows deployers to substitute alternative implementations without changing DAG code.

### Structure conversion tasks (`tasks/supann_2021/`)

Each task receives the full dict of raw LDAP results `{dn: ldap_entry}` and returns `{dn: {field: value}}`.

| Task file | Env var key | Output field(s) |
|---|---|---|
| `convert_ldap_structure_long_labels.py` | `LDAP_STRUCTURE_LONG_LABELS_TASK` | `long_labels` |
| `convert_ldap_structure_short_labels.py` | `LDAP_STRUCTURE_SHORT_LABELS_TASK` | `short_labels` |
| `convert_ldap_structure_descriptions.py` | `LDAP_STRUCTURE_DESCRIPTIONS_TASK` | `descriptions` |
| `convert_ldap_structure_contacts.py` | `LDAP_STRUCTURE_CONTACTS_TASK` | `contacts` |
| `convert_ldap_structure_identifiers.py` | `LDAP_STRUCTURE_IDENTIFIERS_TASK` | `identifiers` |
| `convert_ldap_structure_type.py` | `LDAP_STRUCTURE_TYPE_TASK` | `generic_type`, `main_mission` |
| `convert_ldap_structure_relationships.py` | `LDAP_STRUCTURE_RELATIONSHIPS_TASK` | `relationships` |

### LDAP source fields used for structures

| LDAP attribute | Used by |
|---|---|
| `supannCodeEntite` | dict key (entity identifier) |
| `supannTypeEntite` | identifiers task |
| `supannCodeEntiteParent` | relationships task (`part_of`) |
| `supannRefId` | identifiers task (e.g. `ror:`, `nns:` prefixed) |
| `eduOrgLegalName` | long_labels (primary) |
| `ou` | short_labels (primary), long_labels (fallback) |
| `description` | long_labels (fallback), short_labels (fallback via acronym regex) |
| `businessCategory` | type task → `main_mission` mapping |
| `postalAddress` | contacts task |
| `labeledURI` | contacts task |

### LDAP filter for structures

Structures with `businessCategory=organization` are excluded at query level — they do not map to any IKG structure type.

### AMQP message format

The combined record is wrapped by `send_status_messages` (in `tasks/broadcast/`) into:

```json
{
  "structures_event": {
    "type": "created|updated|unchanged|deleted",
    "data": { <combined fields> }
  }
}
```

The IKG model accepts `contacts` (converted internally to `addresses`/`electronical_addresses`) and `relationships` (converted to `memberships`/`parents`). The `generic_type` field is mandatory.

### `businessCategory` → `main_mission` mapping

| LDAP value | IKG `main_mission` |
|---|---|
| `research` | `research` |
| `administration` | `administrative_services` |
| `library` | `scientific_services` |
| `pedagogy` | `teaching` |
| `organization` | filtered at LDAP level (not ingested) |

## Claude Code commands (`.claude/commands/`)

Three slash commands help edit, validate and visualise the spreadsheet structures CSV before ingestion.

| Command | Spec | Python equivalent |
|---|---|---|
| `/edit-structures <csv> <instruction>` | `edit-structures.md` | — |
| `/check-structures <csv>` | `check-structures.md` | `check_structures.py` |
| `/visualize-structures <csv> [output.html]` | `visualize-structures.md` | `visualize_structures.py` |

**`/edit-structures`** — the how-to guide for editing any structures CSV (the file can live anywhere on disk; `etc/structures_p1ps.csv` is the Paris 1 sample): column semantics, formatting rules (language tags, `local-` prefixes, id conventions), and recipes for the recurring tasks — adding a laboratory's research axes/themes as `team`/`THEME` rows from its website, ignoring structures (with live LDAP lookup of `supannCodeEntite`), and attaching units to their parent UFR. Read it before any manual edit of a structures CSV, even without the slash command.

**`/check-structures`** — validates every row against the full rule set: required fields, `local_id` format, duplicate detection, national-type compatibility (fetched live from the IKG repo), mission rules, conditional identifier requirements, identifier format (ROR/ISNI/NNS), reference integrity, ordering, isolation warnings, date annotation format, and label language tags.

**`/visualize-structures`** — generates a self-contained HTML file (vis.js Network, hierarchical layout) showing the structure hierarchy: inclusion edges (solid), participation edges (dashed), colour-coded by `generic_type`, isolated structures listed below the graph. The template is `structures-visualization.html` in the same directory.

Both Python scripts have no dependencies beyond the standard library and resolve the template path relative to their own location, so they can be run from any working directory:

```bash
python .claude/commands/check_structures.py structures.csv
python .claude/commands/visualize_structures.py structures.csv [output.html]
```

### Key conventions

- All conversion tasks are stateless and pure: they receive a dict of LDAP entries, return a dict of field values.
- Never import Airflow internals directly in task logic — keep task functions importable without a running Airflow instance.
- The `LDAP_DEFAULT_LANGUAGE` env var controls the language tag on all label/description literals.
- `LDAP_STRUCTURE_GENERIC_TYPE` env var sets the `generic_type` for all LDAP-sourced structures (default: `unit`).
