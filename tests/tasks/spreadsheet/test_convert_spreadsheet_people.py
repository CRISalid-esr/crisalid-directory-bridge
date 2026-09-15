import json

import pytest
from airflow.utils.state import TaskInstanceState

from test_utils.dags import create_dag_run, \
    create_task_instance, \
    DATA_INTERVAL_START, \
    DATA_INTERVAL_END

TEST_TASK_ID = "convert_spreadsheet_people"

TESTED_TASK_NAME = 'tasks.spreadsheet.convert_spreadsheet_people.convert_spreadsheet_people'


@pytest.mark.parametrize("dag, expected_result_path", [
    (
            {
                "task_name": TESTED_TASK_NAME,
                "param_names": ["raw_results", "bodies_position_dict"],
                "raw_results": [
                    {
                        'first_names': 'Joe',
                        'last_name': 'Dupond',
                        'main_research_structure': 'U01',
                        'tracking_id': 'jdupond',
                        "eppn": 'jdupond@univ-exemple.fr',
                        'idhali': '',
                        'idhals': '',
                        'orcid': '0000-0000-0000-0001',
                        'idref': '12345678X',
                        'scopus': '',
                    }
                ],
                "bodies_position_dict": {}
            },
            "./tests/data/test_convert_spreadsheet_people.json"
    ),
    (
            {
                "task_name": TESTED_TASK_NAME,
                "param_names": ["raw_results", "bodies_position_dict"],
                "raw_results": [
                    {
                        'first_names': 'Henry',
                        'last_name': 'Gerald',
                        'main_research_structure': 'U85',
                        'tracking_id': 'hgerald',
                        'eppn': '',
                        'idhali': '',
                        'idhals': '',
                        'orcid': '',
                        'idref': '',
                        'scopus': '',
                    }
                ],
                "bodies_position_dict": {}
            },
            "./tests/data/test_convert_spreadsheet_people_with_local_identifier_only.json"
    ),
    (
            {
                "task_name": TESTED_TASK_NAME,
                "param_names": ["raw_results", "bodies_position_dict"],
                "raw_results": [
                    {
                        'first_names': 'Henry',
                        'last_name': 'Gerald',
                        'main_research_structure': 'U85',
                        'tracking_id': 'hgerald',
                        'eppn': 'hgerald@univ-exemple.fr',
                        'idhali': '054235',
                        'idhals': 'henry-gerald',
                        'orcid': '0000-0000-0000-0001',
                        'idref': '012345678X',
                        'scopus': '5432345678X',
                    },
                    {
                        'first_names': 'Joe',
                        'last_name': 'Dupond',
                        'main_research_structure': 'U01',
                        'tracking_id': 'jdupond',
                        'eppn': '',
                        'idhali': '',
                        'idhals': '',
                        'orcid': '',
                        'idref': '',
                        'scopus': '',
                    }
                ],
                "bodies_position_dict": {}
            },
            "./tests/data/test_convert_spreadsheet_with_two_people.json"
    ), (
            {
                "task_name": TESTED_TASK_NAME,
                "param_names": ["raw_results", "bodies_position_dict"],
                "raw_results": [
                    {
                        'first_names': 'Henry,Paul',
                        'last_name': 'Gerald',
                        'main_research_structure': 'U85',
                        'tracking_id': 'hgerald',
                        "eppn": 'hgerald@univ-exemple.fr',
                        'idhali': '054235',
                        'idhals': 'henry-gerald',
                        'orcid': '0000-0000-0000-0001',
                        'idref': '012345678X',
                        'scopus': '5432345678X',
                    }
                ],
                "bodies_position_dict": {}
            },
            "./tests/data/test_convert_spreadsheet_with_two_firstnames.json"
    ),
    (
            {
                "task_name": TESTED_TASK_NAME,
                "param_names": ["raw_results", "bodies_position_dict"],
                "raw_results": [
                    {
                        'first_names': 'Joe',
                        'last_name': 'Dupond',
                        'main_research_structure': '',
                        'tracking_id': 'jdupond',
                        "eppn": 'jdupond@univ-exemple.fr',
                        'idhali': '',
                        'idhals': '',
                        'orcid': '0000-0000-0000-0001',
                        'idref': '12345678X',
                        'scopus': '',
                    }
                ],
                "bodies_position_dict": {}
            },
            "./tests/data/test_convert_spreadsheet_people_with_empty_laboratory.json"
    ),
    (
            {
                "task_name": TESTED_TASK_NAME,
                "param_names": ["raw_results", "bodies_position_dict"],
                "raw_results": [
                    {
                        'first_names': 'Joe',
                        'last_name': 'Dupond',
                        'main_research_structure': 'U01',
                        'tracking_id': 'jdupond',
                        "eppn": 'jdupond@univ-exemple.fr',
                        'idhali': '',
                        'idhals': '',
                        'orcid': '0000-0000-0000-0001',
                        'idref': '12345678X',
                        'scopus': '',
                        'institution_identifier': '1234567X',
                        'institution_id_nomenclature': 'UAI',
                        'position': 'MCF',
                        'employment_start_date': '2010-04-18',
                        'employment_end_date': '2021-06-20'
                    }
                ],
                "bodies_position_dict": {
                    "MCF": "Maître de conférences",
                    "PU": "Professeur des universités"
                }
            },
            "./tests/data/test_convert_spreadsheet_with_employment.json"
    ),
    (
            {
                "task_name": TESTED_TASK_NAME,
                "param_names": ["raw_results", "bodies_position_dict"],
                "raw_results": [
                    {
                        'first_names': 'Joe',
                        'last_name': 'Dupond',
                        'main_research_structure': 'U01',
                        'tracking_id': 'jdupond_stat',
                        'eppn': '',
                        'idhali': '',
                        'idhals': '',
                        'orcid': '',
                        'idref': '',
                        'scopus': '',
                        'membership_type': 'stat_mmb',
                    },
                    {
                        'first_names': 'Joe',
                        'last_name': 'Dupond',
                        'main_research_structure': 'U01',
                        'tracking_id': 'jdupond_assoc',
                        'eppn': '',
                        'idhali': '',
                        'idhals': '',
                        'orcid': '',
                        'idref': '',
                        'scopus': '',
                        'membership_type': 'assoc_mmb',
                    },
                    {
                        'first_names': 'Joe',
                        'last_name': 'Dupond',
                        'main_research_structure': 'U01',
                        'tracking_id': 'jdupond_second',
                        'eppn': '',
                        'idhali': '',
                        'idhals': '',
                        'orcid': '',
                        'idref': '',
                        'scopus': '',
                        'membership_type': 'second_mmb',
                    },
                    {
                        'first_names': 'Joe',
                        'last_name': 'Dupond',
                        'main_research_structure': 'U01',
                        'tracking_id': 'jdupond_visit',
                        'eppn': '',
                        'idhali': '',
                        'idhals': '',
                        'orcid': '',
                        'idref': '',
                        'scopus': '',
                        'membership_type': 'visit_mmb',
                    }
                ],
                "bodies_position_dict": {}
            },
            "./tests/data/test_convert_spreadsheet_people_with_membership_types.json"
    ),
    (
            {
                "task_name": TESTED_TASK_NAME,
                "param_names": ["raw_results", "bodies_position_dict"],
                "raw_results": [
                    {
                        'first_names': 'Joe',
                        'last_name': 'Dupond',
                        'main_research_structure': 'U01',
                        'tracking_id': 'jdupond',
                        "eppn": 'jdupond@univ-exemple.fr',
                        'idhali': '',
                        'idhals': '',
                        'orcid': '0000-0000-0000-0001',
                        'idref': '12345678X',
                        'scopus': '',
                        'contact_email': 'joe.dupond@example.fr',
                        'auth_email': 'jdupond@univ-exemple.fr',
                    }
                ],
                "bodies_position_dict": {}
            },
            "./tests/data/test_convert_spreadsheet_people_with_emails.json"
    ),
    (
            {
                "task_name": TESTED_TASK_NAME,
                "param_names": ["raw_results", "bodies_position_dict"],
                "raw_results": [
                    {
                        'first_names': 'Joe',
                        'last_name': 'Dupond',
                        'main_research_structure': 'U01',
                        'tracking_id': 'jdupond',
                        "eppn": 'jdupond@univ-exemple.fr',
                        'idhali': '',
                        'idhals': '',
                        'orcid': '0000-0000-0000-0001',
                        'idref': '12345678X',
                        'scopus': '',
                        'membership_start_date': '2010-04-18',
                        'membership_end_date': '2021-06-20',
                    }
                ],
                "bodies_position_dict": {}
            },
            "./tests/data/test_convert_spreadsheet_people_with_membership_dates.json"
    ),
    (
            {
                "task_name": TESTED_TASK_NAME,
                "param_names": ["raw_results", "bodies_position_dict"],
                "raw_results": [
                    {
                        'first_names': 'Joe',
                        'last_name': 'Dupond',
                        'main_research_structure': 'U01',
                        'tracking_id': 'jdupond',
                        "eppn": 'jdupond@univ-exemple.fr',
                        'idhali': '',
                        'idhals': '',
                        'orcid': '0000-0000-0000-0001',
                        'idref': '12345678X',
                        'scopus': '',
                        'institution_identifier': '03wnrjx73',
                        'institution_id_nomenclature': 'ROR',
                        'position': 'MCF',
                        'employment_start_date': '2010-04-18',
                        'employment_end_date': '2021-06-20',
                        'hdr': 'no',
                    }
                ],
                "bodies_position_dict": {'MCF': 'Maître de conférences'}
            },
            "./tests/data/test_convert_spreadsheet_with_employment_ror.json"
    ),
    (
            {
                "task_name": TESTED_TASK_NAME,
                "param_names": ["raw_results", "bodies_position_dict"],
                "raw_results": [
                    {
                        'first_names': 'Joe',
                        'last_name': 'Dupond',
                        'main_research_structure': 'U01',
                        'tracking_id': 'jdupond',
                        "eppn": 'jdupond@univ-exemple.fr',
                        'idhali': '',
                        'idhals': '',
                        'orcid': '0000-0000-0000-0001',
                        'idref': '12345678X',
                        'scopus': '',
                        'researcherid': 'B-9809-2012',
                    }
                ],
                "bodies_position_dict": {}
            },
            "./tests/data/test_convert_spreadsheet_people_with_researcherid.json"
    ),
    (
            {
                "task_name": TESTED_TASK_NAME,
                "param_names": ["raw_results", "bodies_position_dict"],
                "raw_results": [
                    {
                        'first_names': 'Joe',
                        'last_name': 'Dupond',
                        'main_research_structure': 'U01',
                        'tracking_id': 'jdupond',
                        "eppn": 'jdupond@univ-exemple.fr',
                        'idhali': '',
                        'idhals': '',
                        'orcid': '0000-0000-0000-0001',
                        'idref': '12345678X',
                        'scopus': '',
                        'membership_type': 'stat_mmb',
                    },
                    {
                        'first_names': 'Joe',
                        'last_name': 'Dupond',
                        'main_research_structure': 'U02',
                        'tracking_id': 'jdupond',
                        "eppn": 'jdupond@univ-exemple.fr',
                        'idhali': '',
                        'idhals': '',
                        'orcid': '0000-0000-0000-0001',
                        'idref': '12345678X',
                        'scopus': '',
                        'membership_type': 'assoc_mmb',
                    }
                ],
                "bodies_position_dict": {}
            },
            "./tests/data/test_convert_spreadsheet_people_with_multiple_memberships.json"
    ),
],
                         indirect=["dag"],
                         ids=[
                             "test_convert_spreadsheet_people",
                             "test_convert_spreadsheet_people_with_local_identifier_only",
                             "test_convert_spreadsheet_with_two_people",
                             "test_convert_spreadsheet_with_two_first_names",
                             "test_convert_spreadsheet_with_empty_laboratory",
                             "test_convert_spreadsheet_with_employment",
                             "test_convert_spreadsheet_people_with_membership_types",
                             "test_convert_spreadsheet_people_with_emails",
                             "test_convert_spreadsheet_people_with_membership_dates",
                             "test_convert_spreadsheet_with_employment_ror",
                             "test_convert_spreadsheet_people_with_researcherid",
                             "test_convert_spreadsheet_people_with_multiple_memberships"
                         ]
                         )
def test_convert_spreadsheet_people(dag, expected_result_path, unique_logical_date):
    """
    Test that the csv data are converted to the expected format
    """
    # pylint: disable=duplicate-code
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_logical_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)

    with open(expected_result_path, 'r', encoding='utf-8') as f:
        expected_result = json.load(f)
    assert ti.state == TaskInstanceState.SUCCESS
    assert ti.xcom_pull(task_ids=TEST_TASK_ID) == expected_result
