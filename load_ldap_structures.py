import logging

import pendulum
from airflow.decorators import dag
from airflow.utils.task_group import TaskGroup

from tasks.combine_results import combine_results
from tasks.database import update_database_task
from tasks.fetch_structures import fetch_structures_task
from utils.config import get_env_variable
from utils.dependencies import import_task

logger = logging.getLogger(__name__)


@dag(
    dag_id="load_ldap_structures",
    start_date=pendulum.datetime(2024, 6, 27, tz="UTC"),
    schedule="@daily",
    catchup=True,
    tags=["structures", "ldap"],
    max_active_tasks=10000,
    default_args={
        "depends_on_past": False,
    }
)
def load_ldap_structures():
    """
    This DAG fetches data from an LDAP server, processes specific fields in parallel,
    and then combines the results into a target JSON structure.
    """
    convert_name_task = import_task(get_env_variable("LDAP_NAME_TASK"))
    convert_acronym_task = import_task(get_env_variable("LDAP_ACRONYM_TASK"))
    convert_description_task = import_task(get_env_variable("LDAP_DESCRIPTION_TASK"))
    convert_address_task = import_task(get_env_variable("LDAP_ADDRESS_TASK"))

    ldap_results = fetch_structures_task()

    with TaskGroup("conversion_tasks"):
        names = convert_name_task.expand(ldap_result=ldap_results)
        acronyms = convert_acronym_task.expand(ldap_result=ldap_results)
        descriptions = convert_description_task.expand(ldap_result=ldap_results)
        addresses = convert_address_task.expand(ldap_result=ldap_results)
    combined_results = combine_results(names, acronyms, descriptions, addresses)
    update_database_task.expand(result=combined_results)


fetch_ldap_structures_dag = load_ldap_structures()
