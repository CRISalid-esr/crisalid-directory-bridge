import logging

import pendulum
from airflow.decorators import dag
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup

from tasks.combine_batch_results import combine_batch_results
from tasks.database import update_database, create_redis_connection
from tasks.fetch_from_spreadsheet import fetch_from_spreadsheet
from tasks.fetch_people_from_ldap import fetch_ldap_people
from tasks.supann_2021.complete_identifiers import complete_identifiers
from utils.config import get_env_variable
from utils.dependencies import import_from_path

logger = logging.getLogger(__name__)


@dag(
    dag_id="load_ldap_people",
    start_date=pendulum.datetime(2024, 7, 2, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["people", "ldap"],
    max_active_tasks=10,
    default_args={
        "depends_on_past": False,
    }
)
def load_ldap_people():
    """
    This DAG fetches data from an LDAP server, processes specific fields,
    and then combines the results into a target JSON structure.
    """
    entity_source = "ldap"
    entity_type = "people"
    task_keys = ["NAME", "IDENTIFIER", "MEMBERSHIP", "EMPLOYMENT"]
    tasks = {}

    for key in task_keys:
        tasks[key] = import_from_path(get_env_variable(f"LDAP_PERSON_{key}_TASK"))

    connexion = create_redis_connection()
    ldap_results = fetch_ldap_people()

    # pylint: disable=duplicate-code
    trigger_broadcast = TriggerDagRunOperator(
        task_id='trigger_broadcast',
        trigger_dag_id='broadcast_entities',
        logical_date="{{ logical_date + macros.timedelta(seconds=20) }}",
        trigger_run_id='ldap_people_run_{{ logical_date.int_timestamp }}',
        conf={
            "timestamp": "{{ logical_date.int_timestamp }}",
            "entity_type": entity_type,
            "entity_source": entity_source,
        },
        wait_for_completion=False,
    )

    batch_results = []
    # pylint: disable=duplicate-code, unexpected-keyword-arg
    with TaskGroup(group_id="people_fields_conversion_tasks",
                   group_display_name="People fields conversion tasks"):
        for key, task in tasks.items():
            converted_result = task(ldap_results=ldap_results)
            batch_results.append(converted_result)
    combined_results = combine_batch_results(batch_results)
    if get_env_variable("COMPLETE_LDAP_PEOPLE_IDENTIFIERS_FROM_SPREADSHEET"):
        identifiers_from_spreadsheet = fetch_from_spreadsheet(entity_source, entity_type)
        completed_results = complete_identifiers(
            ldap_source=combined_results,
            identifiers_spreadsheet=identifiers_from_spreadsheet
        )
    else:
        completed_results = combined_results
    redis_keys = update_database(result=completed_results, prefix=f"{entity_type}:{entity_source}:")
    connexion >> redis_keys >> trigger_broadcast  # pylint: disable=pointless-statement # pylint: disable=pointless-statement


load_ldap_people()
