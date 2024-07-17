import logging

import pendulum
from airflow.decorators import dag
from airflow.utils.task_group import TaskGroup

from tasks.combine_batch_results import combine_batch_results
from tasks.database import update_database, create_redis_connection_task
from tasks.fetch_people import fetch_people_task
from utils.config import get_env_variable
from utils.dependencies import import_from_path

logger = logging.getLogger(__name__)


@dag(
    dag_id="load_ldap_people",
    start_date=pendulum.datetime(2024, 7, 2, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["people", "ldap"],
    max_active_tasks=100,
    default_args={
        "depends_on_past": False,
    }
)
def load_ldap_people():
    """
    This DAG fetches data from an LDAP server, processes specific fields in parallel,
    and then combines the results into a target JSON structure.
    """
    task_keys = ["NAME", "IDENTIFIER"]
    tasks = {}

    for key in task_keys:
        tasks[key] = import_from_path(get_env_variable(f"LDAP_PERSON_{key}_TASK"))

    connexion = create_redis_connection_task()
    ldap_results = fetch_people_task()

    batch_results = []
    with TaskGroup("people_fields_conversion_tasks"):
        for key, task in tasks.items():
            converted_result = task(ldap_results=ldap_results)
            batch_results.append(converted_result)
    combined_results = combine_batch_results(batch_results)
    redis_keys = update_database(result=combined_results, prefix="pers:ldap:")
    connexion >> redis_keys  # pylint: disable=pointless-statement


load_ldap_people()
