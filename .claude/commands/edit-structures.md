Edit a structures CSV file following the instruction in `$ARGUMENTS` (which contains the path to the CSV — anywhere on disk — and the requested change; if no path is given and none is obvious from the conversation, ask which file to edit).

This command starts an **editing session**: after it has been invoked once, treat every subsequent user message as a new instruction on the same CSV under the same rules (short messages like "CESSP : UR11" mean "attach CESSP to UR11", "ignore X" means recipe 2), without the user re-invoking the command. Switch files only when the user gives a different path.

This command teaches you the logic and conventions of the structures spreadsheet so you can perform the recurring editing tasks: adding research teams/axes from a laboratory website, marking structures as ignored, and attaching units to their parent in the hierarchy.

---

## What this file is

The structures CSV is the **manual override layer** for the `load_ldap_structures` DAG (see `spec/120-extend-ldap-structure-dag/prompt.md`). LDAP provides the raw institutional structures; this CSV enriches, corrects, adds, or drops them before broadcast to the CRISalid IKG. The target data model (generic types, national types, relationships) is specified in the IKG repo: `~/PycharmProjects/crisalid-ikg/specs/new-research-structure-model#373/prompt.md`.

Merge semantics (per `local_id`):
- Row matches an LDAP entry → non-empty CSV fields overwrite LDAP fields; empty CSV fields keep LDAP values.
- Row has no LDAP match → added as a new structure (normal and expected, e.g. teams).
- `generic_type=ignore` → the structure is **dropped entirely**, even if present in LDAP.

## Column reference

`generic_type,type,local_types,main_mission,secondary_missions,local_id,short_labels,long_labels,descriptions,inclusions,participations,uai,nns,ror,isni,wikidata,scopus,erc_research_field,hceres_research_areas,hal_collection,web,signature,campus,city_name,city_code,city_adress`

- **generic_type**: `institution` | `institution_subdivision` | `unit` | `team` | `ignore`. Mandatory.
- **type** (national type): `UNIV`, `EPE`, `COMUE`, `UMR`, `UAR`, `UR`, `IRL`, `UFR`, `FAC`, `TEAM`, `THEME`. Must be compatible with generic_type (teams → `TEAM` or `THEME`; units → `UMR`/`UAR`/`UR`/`IRL`; subdivisions → `UFR`/`FAC`). The authoritative list lives in the IKG enums and evolves.
- **local_types**: free-form local designations as literals with language tags, `|`-separated: `Axe[fr]|Axis[en]`. Use the *terminology of the source website* (Axe, Thème transversal, Socle, Pôle thématique, Chantier transversal, Groupe de recherche, Département…).
- **main_mission**: only for `unit` rows: `research` | `teaching` | `administrative_services` | `scientific_services`. Leave empty for institutions, subdivisions, and teams.
- **local_id**: the LDAP `supannCodeEntite` when the structure exists in LDAP; a synthetic id otherwise. Convention for teams: `<PARENT_LOCAL_ID>_T1`, `_T2`, … (e.g. `U029_T3` for PHARE's third axis). Must be unique in the file.
- **short_labels / long_labels / descriptions**: literals with language tag suffix `[fr]` / `[en]`, `|`-separated for several languages. Convention for teams: short label = bare title (or the lab's acronym for the axis if it has one), long label = title with its local-type prefix as printed on the site (`Axe 1 : …`, `Thème transversal 2 : …`, `Socle …`).
- **inclusions**: strong hierarchy (part_of), child → parent, format `local-<PARENT_LOCAL_ID>`. Teams point to their laboratory; research units point to their UFR/école (e.g. `local-U02`).
- **participations**: weak links (member_of), `|`-separated, with optional subtype suffix: `local-UP1[main_supervision]|ror-02feahw73[main_supervision]|uai-0921204J[associated_supervision]`. Subtypes: `main_supervision`, `associated_supervision`, `participating_supervision`. Supervision entries are about institutions↔units — do not touch them when only changing hierarchy.
- **uai/nns/ror/isni/wikidata/scopus**: external identifiers (research units should have `nns`; institutions need `uai`).
- **city_adress**: `$`-separated: `Building$Street$Zip City$Country`.

## Hard rules and gotchas

- Reference prefixes are **strictly lowercase**: `local-`, `ror-`, `uai-`. `Local-XXX` is silently broken (structure becomes an orphan).
- A parent must be **declared on an earlier row** than the rows referencing it.
- Fields containing commas must be double-quoted (standard CSV).
- Some existing lines contain **non-breaking spaces** (e.g. before `:` in French labels), so exact-string edits may fail; prefer Python line-based edits keyed on `local_id`, or anchor edits on ASCII-only fragments.
- Teams representing research *axes/thèmes* use national type `THEME`; teams representing actual staffed sub-teams/departments use `TEAM`.
- When rewriting rows programmatically, edit lines in place (don't re-serialize the whole file) to avoid noisy quoting diffs.

## Recurring task recipes

### 1. Add a laboratory's research axes/themes as teams

1. Find the lab's row: `grep -n "<ACRONYM>" <csv>` → note its `local_id` (e.g. PHARE = `U029`, ACTE = `UR049_4`).
2. Fetch the lab page given by the user (usually `…/axes-recherche`). If the listing only has titles, fetch each axis's own subpage for its description. If the site has a TLS problem (e.g. `sphere.cnrs.fr`), fall back to `curl -sk`.
3. If the site is English-only, provide bilingual labels: `Ingénierie des exigences[fr]|Requirements Engineering[en]`.
4. One row per axis/theme, appended at the end of the file:
   - `generic_type=team`, `type=THEME`
   - `local_types` mirroring the site's own wording, fr + en
   - `local_id=<LAB_ID>_Tn` (continue numbering if some already exist)
   - description: a faithful 2–3 sentence French summary of the page content, tagged `[fr]`
   - `inclusions=local-<LAB_ID>`; everything else empty
5. Skip listing items that are not research axes (projects, publications, resource platforms) — mention them to the user instead.

Example row:
```
team,THEME,Axe[fr]|Axis[en],,,U029_T5,Colonisations[fr],Axe Colonisations[fr],"<résumé>[fr]",local-U029,,,,,,,,,,,,,,,,
```

### 2. Ignore a structure

Set `generic_type` to `ignore` (keep the rest of the row). If the structure is not in the CSV yet, it must be added with its **real LDAP code** — never invent a `local_id`. Look it up live:

```bash
python - <<'EOF'
import os, json
from utils.ldap import connect_to_ldap
conn = connect_to_ldap()
conn.search(os.environ["LDAP_STRUCTURES_BRANCH"], "(ou=<SHORT_NAME>)",
            attributes=["supannCodeEntite","ou","description","supannCodeEntiteParent","businessCategory"])
for e in conn.entries: print(e.entry_to_json())
EOF
```
(run from the crisalid-directory-bridge repo root with its virtualenv; LDAP connection settings come from the environment / `.env.dev`)

"Ignore X and all its children" means every row whose short label is `X` or starts with `X-`, including nested descendants.

### 3. Attach a unit to a parent (hierarchy)

Set the `inclusions` field of the child row to `local-<PARENT_ID>`. Find the parent's `local_id` by grepping the file for its acronym; users often refer to parents by their national type + number (e.g. "UFR 08", "UR 04") rather than by `local_id` — resolve against the actual rows. Leave `participations` (supervisions) untouched.

## Always validate

Run the checker **before your first change** to record the file's pre-existing baseline of errors/warnings, then after **every** change:
```bash
python .claude/commands/check_structures.py <csv>
```
(the script resolves its data relative to its own location, so it works on a CSV anywhere on disk). Any *new* error or warning relative to the baseline must be fixed before finishing; pre-existing ones are the file owner's business — report them, don't silently fix. `python .claude/commands/visualize_structures.py <csv> [out.html]` renders the hierarchy for visual checking.
