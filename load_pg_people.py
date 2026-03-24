"""
DAG to load people data from PostgreSQL va_people_crisalid view
and convert it to ESUP-Portail v2 format.

This DAG:
1. Fetches data from PostgreSQL view (va_people_crisalid)
2. Converts data using the same ESUP-Portail transformation pipeline
3. Stores results in Redis with 'people:postgresql:' prefix
4. Triggers the broadcast_entities DAG to propagate changes
"""
import logging

import pendulum
from airflow.sdk import dag
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from tasks.database import update_database, create_redis_connection
from tasks.fetch_from_postgresql import fetch_from_postgresql_view
from tasks.spreadsheet.convert_spreadsheet_people import convert_spreadsheet_people
from tasks.fetch_from_employee_types import employee_type_labels_by_codes
from utils.config import get_env_variable
from utils.yaml_loader import load_yaml

logger = logging.getLogger(__name__)


@dag(
    dag_id="load_pg_people",
    start_date=pendulum.datetime(2024, 7, 2, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["people", "postgresql"],
    max_active_tasks=10,
    default_args={
        "depends_on_past": False,
    }
)
def load_pg_people():
    """
    Load people from PostgreSQL view and convert to ESUP-Portail v2 format.
    
    This DAG fetches data from the va_people_crisalid view which aggregates:
    - Individual information (prenom, nom, edupersonprincipalname, mail)
    - RH affectations (main research structure)
    - Identifiers (HAL, ORCID, IdRef)
    
    The data is then converted using the same P0-P4 transformation pipeline:
    - P0: membership_type validation
    - P1: email extraction
    - P2: membership dates
    - P3: nomenclature (UAI vs ROR prefixes)
    - P4: researcherid (WOS identifier)
    """
    entity_source = "postgresql"
    entity_type = "people"

    # Step 1: Create Redis connection
    connexion = create_redis_connection()
    
    # Step 2: Fetch data from PostgreSQL view
    people_source_data = fetch_from_postgresql_view(view_name="va_people_crisalid")
    
    # Step 3: Load employee type configuration
    employee_types = load_yaml(get_env_variable("YAML_EMPLOYEE_TYPE_PATH"))
    bodies_position = employee_type_labels_by_codes(employee_types)

    # Step 4: Define broadcast trigger (will run after conversion)
    trigger_broadcast = TriggerDagRunOperator(
        task_id='trigger_broadcast',
        trigger_dag_id='broadcast_entities',
        logical_date="{{ logical_date + macros.timedelta(seconds=30) }}",
        trigger_run_id='pg_people_run_{{ logical_date.int_timestamp }}',
        conf={
            "timestamp": "{{ logical_date.int_timestamp }}",
            "entity_type": entity_type,
            "entity_source": entity_source,
        },
        wait_for_completion=False,
    )

    # Step 5: Convert data using ESUP-Portail transformation pipeline
    converted_result = convert_spreadsheet_people(
        source_data=people_source_data,
        config=bodies_position
    )
    
    # Step 6: Store in Redis with postgresql prefix
    redis_keys = update_database(
        result=converted_result,
        prefix=f"{entity_type}:{entity_source}:"  # people:postgresql:
    )
    
    # Step 7: Define task dependencies
    connexion >> redis_keys >> trigger_broadcast  # pylint: disable=pointless-statement


load_pg_people()
