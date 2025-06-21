import logging

import pendulum
from airflow.decorators import dag
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from tasks.database import update_database, create_redis_connection
from tasks.fetch_from_spreadsheet import fetch_from_spreadsheet
from tasks.spreadsheet.convert_spreadsheet_structures import convert_spreadsheet_structures

logger = logging.getLogger(__name__)


@dag(
    dag_id="load_spreadsheet_structures",
    start_date=pendulum.datetime(2024, 7, 2, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["structures", "spreadsheet"],
    max_active_tasks=10,
    default_args={
        "depends_on_past": False,
    }
)
def load_spreadsheet_structures():
    """
    This DAG fetches data from a spreadsheet,
    processes specific fields,
    and then combines the results into a target JSON structure.
    """
    entity_source = "spreadsheet"
    entity_type = "structures"

    connexion = create_redis_connection()
    structures_source_data = fetch_from_spreadsheet(entity_source, entity_type)

    # pylint: disable=duplicate-code
    trigger_broadcast = TriggerDagRunOperator(
        task_id='trigger_broadcast',
        trigger_dag_id='broadcast_entities',
        logical_date="{{ logical_date + macros.timedelta(seconds=20) }}",
        trigger_run_id='spreadsheet_structures_run_{{ logical_date.int_timestamp }}',
        conf={
            "timestamp": "{{ logical_date.int_timestamp }}",
            "entity_type": entity_type,
            "entity_source": entity_source,
        },
        wait_for_completion=False,
    )

    converted_result = convert_spreadsheet_structures(source_data=structures_source_data)
    redis_keys = update_database(result=converted_result, prefix=f"{entity_type}:{entity_source}:")
    connexion >> redis_keys >> trigger_broadcast  # pylint: disable=pointless-statement


load_spreadsheet_structures()
