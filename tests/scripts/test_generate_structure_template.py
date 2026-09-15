import pytest

from scripts.generate_structure_template import ldap_dict_to_csv_rows
from tests.data.ldap.structures_ldap_response import LDAP_STRUCTURES_RESPONSE

LANG = "fr"
GENERIC_TYPE = "unit"


def _rows():
    return ldap_dict_to_csv_rows(LDAP_STRUCTURES_RESPONSE, lang=LANG, generic_type=GENERIC_TYPE)


def _row(code):
    return next(r for r in _rows() if r["local_id"] == code)


def test_basic_output_shape():
    """One row per LDAP entry, all required columns present."""
    rows = _rows()
    assert len(rows) == len(LDAP_STRUCTURES_RESPONSE)
    required_columns = {
        "generic_type", "type", "local_types", "main_mission", "secondary_missions",
        "local_id", "short_labels", "long_labels", "descriptions", "inclusions",
        "participations", "uai", "nns", "ror", "isni", "wikidata", "scopus",
        "erc_research_field", "hceres_research_areas", "hal_collection",
        "web", "signature", "campus",
    }
    for row in rows:
        assert required_columns.issubset(row.keys())


def test_ldap_derivable_fields():
    """LDAP-derivable fields are pre-filled for TEST_U1."""
    row = _row("TEST_U1")
    assert row["local_id"] == "TEST_U1"
    assert row["generic_type"] == "unit"
    assert row["main_mission"] == "research"
    assert row["inclusions"] == "local-TEST_PARENT"
    assert row["short_labels"] != ""
    assert row["long_labels"] != ""
    assert row["descriptions"] != ""
    assert row["web"] != ""


def test_language_tag():
    """Labels carry [fr] language tag."""
    row = _row("TEST_U1")
    assert row["short_labels"].endswith("[fr]")
    assert row["long_labels"].endswith("[fr]")
    assert row["descriptions"].endswith("[fr]")


def test_long_label_fallback_to_ou():
    """When eduOrgLegalName absent, long_labels falls back to ou."""
    row = _row("TEST_U4")
    assert row["eduOrgLegalName"] if "eduOrgLegalName" in row else True
    assert "Unité Sans Nom Légal[fr]" == row["long_labels"]


def test_empty_fields():
    """Fields requiring human input are empty strings."""
    row = _row("TEST_U1")
    for field in ("type", "local_types", "ror", "nns", "wikidata", "uai",
                  "isni", "scopus", "hal_collection", "signature", "campus"):
        assert row[field] == "", f"Expected empty string for {field}, got {row[field]!r}"


def test_parent_ref():
    """supannCodeEntiteParent → inclusions formatted as local-{code}."""
    row = _row("TEST_U1")
    assert row["inclusions"] == "local-TEST_PARENT"


def test_no_parent():
    """No supannCodeEntiteParent → inclusions is empty string."""
    row = _row("TEST_U3")
    assert row["inclusions"] == ""


def test_organization_business_category():
    """businessCategory=organization → main_mission is empty (not mapped)."""
    row = _row("TEST_U5")
    assert row["main_mission"] == ""


def test_administration_mapping():
    """businessCategory=administration → main_mission=administrative_services."""
    row = _row("TEST_U2")
    assert row["main_mission"] == "administrative_services"


def test_web_label_stripped():
    """labeledURI label suffix is stripped, only URI kept."""
    row = _row("TEST_U1")
    assert row["web"] == "https://www.ltu1.example.fr"
    assert " " not in row["web"]
