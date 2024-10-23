import logging

import pendulum
from airflow.decorators import dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup

from tasks.combine_batch_results import combine_batch_results
from tasks.database import update_database, create_redis_connection
from tasks.fetch_structures_from_ldap import fetch_structures_from_ldap
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
    task_keys = ["NAME", "ACRONYM", "DESCRIPTION", "ADDRESS", "IDENTIFIER"]
    tasks = {}

    for key in task_keys:
        tasks[key] = import_from_path(get_env_variable(f"LDAP_STRUCTURE_{key}_TASK"))

    connexion = create_redis_connection()
    ldap_results = fetch_structures_from_ldap()
    # pylint: disable=duplicate-code
    trigger_broadcast = TriggerDagRunOperator(
        task_id='trigger_broadcast',
        trigger_dag_id='broadcast_entities',
        execution_date="{{ execution_date + macros.timedelta(seconds=10) }}",
        trigger_run_id='ldap_structures_run_{{ execution_date.int_timestamp }}',
        conf={
            "timestamp": "{{ execution_date.int_timestamp }}",
            "entity_type": entity_type,
            "entity_source": entity_source,
        },
        wait_for_completion=False,
    )

    batch_results = []
    # pylint: disable=duplicate-code
    with TaskGroup("structure_fields_conversion_tasks"):
        for key, task in tasks.items():
            converted_result = task(ldap_results=ldap_results)
            batch_results.append(converted_result)
    combined_results = combine_batch_results(batch_results)
    redis_keys = update_database(result=combined_results, prefix=f"{entity_type}:{entity_source}:")
    connexion >> redis_keys >> trigger_broadcast  # pylint: disable=pointless-statement


load_ldap_structures()
