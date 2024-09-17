import os

from read_spreadsheet import fetch_people_from_spreadsheet, fetch_structures_from_spreadsheet


def test_fetch_people_from_spreadsheet(monkeypatch):
    monkeypatch.setenv("SPREADSHEET_PEOPLE_PATH", "./tests/data/csv/people.csv")
    assert os.getenv("SPREADSHEET_PEOPLE_PATH") == "./tests/data/csv/people.csv"
    expected_result = [{'first_name': 'Joe',
                        'idHal_i': "012345678",
                        'idHal_s': 'jean-dupond',
                        'idref': "012345678",
                        'last_name': 'Dupond',
                        'local_identifier': 'jdupond',
                        'main_laboratory_identifier': 'U01',
                        'orcid': '0000-0000-0000-0001',
                        'scopus_eid': "012345678"}]
    result = fetch_people_from_spreadsheet()
    assert result == expected_result


def test_fetch_structures_from_spreadsheet(monkeypatch):
    monkeypatch.setenv("SPREADSHEET_STRUCTURES_PATH", "./tests/data/csv/structures.csv")
    assert os.getenv("SPREADSHEET_STRUCTURES_PATH") == "./tests/data/csv/structures.csv"
    expected_result = [{'RNSR': '0199812919',
                        'ROR': '01296475',
                        'acronym': '',
                        'city_adress': 'Centre Meudon, 1 PLACE ARISTIDE BRIAND',
                        'city_code': "92190",
                        'city_name': 'MEUDON',
                        'description': '',
                        'local_identifier': 'U082',
                        'name': 'Laboratoire de géographie physique Pierre Birot (UMR 8591)'}]
    result = fetch_structures_from_spreadsheet()
    assert result == expected_result