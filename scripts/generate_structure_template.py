"""
Generate a CSV template pre-filled from live LDAP data.

Usage (from anywhere):
    APP_ENV=DEV python scripts/generate_structure_template.py --output data/structures_template.csv

Credentials and LDAP parameters are read from the environment (.env.dev when APP_ENV=DEV).
--output defaults to stdout.
"""
import argparse
import csv
import os
import sys

# Ensure the repo root is on sys.path regardless of how the script is invoked
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging

# utils/config.py warns about the absence of a bare .env file at import time.
# We use .env.dev — silence that warning before the import triggers it.
logging.getLogger("utils.config").setLevel(logging.ERROR)

from ldap3 import SUBTREE
from ldap3.core.exceptions import LDAPExceptionError

from utils.exceptions import LDAPError
from utils.ldap import connect_to_ldap, ldap_response_to_json_dict

BUSINESS_CATEGORY_TO_MAIN_MISSION = {
    "administration": "administrative_services",
    "library": "scientific_services",
    "research": "research",
    "pedagogy": "teaching",
}

CSV_COLUMNS = [
    "generic_type",
    "type",
    "local_types",
    "main_mission",
    "secondary_missions",
    "local_id",
    "short_labels",
    "long_labels",
    "descriptions",
    "inclusions",
    "participations",
    "uai",
    "nns",
    "ror",
    "isni",
    "wikidata",
    "scopus",
    "erc_research_field",
    "hceres_research_areas",
    "hal_collection",
    "web",
    "signature",
    "campus",
    "city_name",
    "city_code",
    "city_adress",
]


def _first(values):
    """Return first element of a list, or empty string."""
    if values and isinstance(values, list):
        return values[0] or ""
    return ""


def _strip_labeleduri_label(uri_str):
    """Strip trailing label from labeledURI value (format: 'URI optional label')."""
    if not uri_str:
        return ""
    return uri_str.split(" ")[0]


def ldap_dict_to_csv_rows(ldap_data: dict, lang: str, generic_type: str) -> list[dict]:
    """Convert LDAP response dict to a list of CSV row dicts."""
    rows = []
    for code, entry in ldap_data.items():
        ou = _first(entry.get("ou", []))
        acronym = _first(entry.get("acronym", []))
        edu_org = _first(entry.get("eduOrgLegalName", []))
        description = _first(entry.get("description", []))
        labeled_uri = _first(entry.get("labeledURI", []))
        postal_address = _first(entry.get("postalAddress", []))
        parent = _first(entry.get("supannCodeEntiteParent", []))
        business_category = _first(entry.get("businessCategory", []))

        short_label = acronym or ou
        long_label = edu_org or ou

        main_mission = BUSINESS_CATEGORY_TO_MAIN_MISSION.get(business_category, "")

        rows.append({
            "generic_type": generic_type,
            "type": "",
            "local_types": "",
            "main_mission": main_mission,
            "secondary_missions": "",
            "local_id": code,
            "short_labels": f"{short_label}[{lang}]" if short_label else "",
            "long_labels": f"{long_label}[{lang}]" if long_label else "",
            "descriptions": f"{description}[{lang}]" if description else "",
            "inclusions": f"local-{parent}" if parent else "",
            "participations": "",
            "uai": "",
            "nns": "",
            "ror": "",
            "isni": "",
            "wikidata": "",
            "scopus": "",
            "erc_research_field": "",
            "hceres_research_areas": "",
            "hal_collection": "",
            "web": _strip_labeleduri_label(labeled_uri),
            "signature": "",
            "campus": "",
            "city_name": "",
            "city_code": "",
            "city_adress": postal_address,
        })
    return rows


def _fetch_ldap_structures():
    """Fetch structures from LDAP and return as dict keyed by supannCodeEntite."""
    ldap_connexion = connect_to_ldap()
    structures_branch = os.environ["LDAP_STRUCTURES_BRANCH"]
    structures_filter = os.environ["LDAP_STRUCTURES_FILTER"]

    try:
        ldap_connexion.search(
            search_base=structures_branch,
            search_filter=structures_filter,
            search_scope=SUBTREE,
            attributes=[
                "supannCodeEntite",
                "supannTypeEntite",
                "supannCodeEntiteParent",
                "eduOrgLegalName",
                "ou",
                "description",
                "acronym",
                "postalAddress",
                "labeledURI",
                "supannRefId",
                "businessCategory",
            ],
        )
        ldap_response = ldap_connexion.entries
    except LDAPExceptionError as error:
        raise LDAPError("Unable to fetch structures from LDAP") from error

    return ldap_response_to_json_dict(ldap_response, dict_key="supannCodeEntite")


def main():
    parser = argparse.ArgumentParser(description="Generate structure CSV template from live LDAP.")
    parser.add_argument("--output", default="-", help="Output file path (default: stdout)")
    args = parser.parse_args()

    lang = os.environ.get("LDAP_DEFAULT_LANGUAGE", "fr")
    generic_type = os.environ.get("LDAP_STRUCTURE_GENERIC_TYPE", "unit")

    ldap_data = _fetch_ldap_structures()
    rows = ldap_dict_to_csv_rows(ldap_data, lang=lang, generic_type=generic_type)

    if args.output == "-":
        writer = csv.DictWriter(sys.stdout, fieldnames=CSV_COLUMNS)
    else:
        f = open(args.output, "w", newline="", encoding="utf-8")
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)

    writer.writeheader()
    writer.writerows(rows)

    if args.output != "-":
        f.close()


if __name__ == "__main__":
    main()
