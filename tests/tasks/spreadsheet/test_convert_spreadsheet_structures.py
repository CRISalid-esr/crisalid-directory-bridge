import pytest
from airflow.utils.state import TaskInstanceState

from test_utils.dags import create_dag_run, \
    DATA_INTERVAL_START, \
    DATA_INTERVAL_END, \
    create_task_instance

TEST_TASK_ID = "convert_spreadsheet_structures"

TESTED_TASK_NAME = 'tasks.spreadsheet.convert_spreadsheet_structures.convert_spreadsheet_structures'


def _row(local_id: str, hal_collection: str) -> dict:
    return {
        'generic_type': 'unit',
        'type': 'UMR',
        'local_id': local_id,
        'short_labels': f'{local_id}[fr]',
        'long_labels': f'{local_id}[fr]',
        'descriptions': '',
        'inclusions': '',
        'participations': '',
        'uai': '',
        'nns': '',
        'ror': '',
        'isni': '',
        'wikidata': '',
        'scopus': '',
        'erc_research_field': '',
        'hceres_research_areas': '',
        'hal_collection': hal_collection,
        'web': '',
        'signature': '',
        'campus': '',
        'main_mission': 'research',
        'secondary_missions': '',
        'local_types': ''
    }


@pytest.mark.parametrize("dag", [
    {
        "task_name": TESTED_TASK_NAME,
        "param_names": ["raw_results"],
        "raw_results": [_row('U1', '  https://hal.science/COLLECTION  ')]
    }
], indirect=['dag'])
def test_hal_collection_is_converted(dag, unique_logical_date) -> None:
    """A non-empty hal_collection cell is stripped and set as a top-level field."""
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_logical_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)

    assert ti.state == TaskInstanceState.SUCCESS
    result = ti.xcom_pull(task_ids=TEST_TASK_ID)
    assert result['U1']['hal_collection'] == 'https://hal.science/COLLECTION'
    assert all(i['type'] != 'hal_collection' for i in result['U1']['identifiers'])


@pytest.mark.parametrize("dag", [
    {
        "task_name": TESTED_TASK_NAME,
        "param_names": ["raw_results"],
        "raw_results": [_row('U1', ''), _row('U2', '   ')]
    }
], indirect=['dag'])
def test_empty_hal_collection_is_omitted(dag, unique_logical_date) -> None:
    """An empty or whitespace-only hal_collection cell leaves the field out of the record."""
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_logical_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)

    assert ti.state == TaskInstanceState.SUCCESS
    result = ti.xcom_pull(task_ids=TEST_TASK_ID)
    assert 'hal_collection' not in result['U1']
    assert 'hal_collection' not in result['U2']
