import logging

import pendulum
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag, TaskGroup

from tasks.combine_batch_results import combine_batch_results
from tasks.database import update_database, create_redis_connection
from tasks.fetch_from_spreadsheet import fetch_from_spreadsheet
from tasks.fetch_structures_from_ldap import fetch_structures_from_ldap
from tasks.spreadsheet.convert_spreadsheet_structures import convert_spreadsheet_structures
from tasks.supann_2021.override_ldap_structure_data import override_ldap_structure_data
from utils.config import get_env_variable
from utils.dependencies import import_from_path

logger = logging.getLogger(__name__)


@dag(
    dag_id="load_ldap_structures",
    start_date=pendulum.datetime(2024, 7, 2, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["structures", "ldap"],
    max_active_tasks=10,
    default_args={
        "depends_on_past": False,
    }
)
def load_ldap_structures():
    """
    This DAG fetches data from an LDAP server, processes specific fields in parallel,
    and then combines the results into a target JSON structure.
    """
    entity_type = "structures"
    entity_source = "ldap"
    task_keys = ["LONG_LABELS", "SHORT_LABELS", "DESCRIPTIONS", "CONTACTS", "IDENTIFIERS", "TYPE",
                 "RELATIONSHIPS"]
    tasks = {}

    for key in task_keys:
        tasks[key] = import_from_path(get_env_variable(f"LDAP_STRUCTURE_{key}_TASK"))

    connexion = create_redis_connection()
    ldap_results = fetch_structures_from_ldap()
    # pylint: disable=duplicate-code
    trigger_broadcast = TriggerDagRunOperator(
        task_id='trigger_broadcast',
        trigger_dag_id='broadcast_entities',
        logical_date="{{ logical_date + macros.timedelta(seconds=10) }}",
        trigger_run_id='ldap_structures_run_{{ logical_date.int_timestamp }}',
        conf={
            "timestamp": "{{ logical_date.int_timestamp }}",
            "entity_type": entity_type,
            "entity_source": entity_source,
        },
        wait_for_completion=False,
    )

    batch_results = []
    # pylint: disable=duplicate-code,unexpected-keyword-arg
    with TaskGroup(group_id="structure_fields_conversion_tasks",
                   group_display_name="Structure fields conversion tasks"):
        for key, task in tasks.items():
            converted_result = task(ldap_results=ldap_results)
            batch_results.append(converted_result)
    combined_results = combine_batch_results(batch_results)
    if get_env_variable("OVERRIDE_LDAP_STRUCTURE_DATA_FROM_SPREADSHEET"):
        raw_spreadsheet = fetch_from_spreadsheet(entity_source, entity_type)
        spreadsheet_structures = convert_spreadsheet_structures(source_data=raw_spreadsheet)
        final_results = override_ldap_structure_data(
            ldap_source=combined_results,
            spreadsheet_source=spreadsheet_structures,
        )
    else:
        final_results = combined_results
    redis_keys = update_database(result=final_results, prefix=f"{entity_type}:{entity_source}:")
    connexion >> redis_keys >> trigger_broadcast  # pylint: disable=pointless-statement


load_ldap_structures()
