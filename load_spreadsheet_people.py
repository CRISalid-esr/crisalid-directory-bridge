import logging

import pendulum
from airflow.decorators import dag
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from tasks.database import update_database, create_redis_connection
from tasks.fetch_from_spreadsheet import fetch_from_spreadsheet
from tasks.spreadsheet.convert_spreadsheet_people import convert_spreadsheet_people
from tasks.fetch_from_employee_types import (
    employee_type_labels_by_codes,
)
from utils.config import get_env_variable
from utils.yaml_loader import load_yaml

logger = logging.getLogger(__name__)


@dag(
    dag_id="load_spreadsheet_people",
    start_date=pendulum.datetime(2024, 7, 2, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["people", "spreadsheet"],
    max_active_tasks=10,
    default_args={
        "depends_on_past": False,
    }
)
def load_spreadsheet_people():
    """
    This DAG fetches data from a spreadsheet,
    processes specific fields,
    and then combines the results into a target JSON structure.
    """
    entity_source = "spreadsheet"
    entity_type = "people"

    connexion = create_redis_connection()
    people_source_data = fetch_from_spreadsheet(entity_source, entity_type)
    employee_types = load_yaml(get_env_variable("YAML_EMPLOYEE_TYPE_PATH"))
    bodies_position = employee_type_labels_by_codes(employee_types)

    # pylint: disable=duplicate-code
    trigger_broadcast = TriggerDagRunOperator(
        task_id='trigger_broadcast',
        trigger_dag_id='broadcast_entities',
        logical_date="{{ logical_date + macros.timedelta(seconds=30) }}",
        trigger_run_id='spreadsheet_people_run_{{ logical_date.int_timestamp }}',
        conf={
            "timestamp": "{{ logical_date.int_timestamp }}",
            "entity_type": entity_type,
            "entity_source": entity_source,
        },
        wait_for_completion=False,
    )

    converted_result = convert_spreadsheet_people(source_data=people_source_data,
                                                  config=bodies_position)
    redis_keys = update_database(result=converted_result, prefix=f"{entity_type}:{entity_source}:")
    connexion >> redis_keys >> trigger_broadcast  # pylint: disable=pointless-statement


load_spreadsheet_people()
