"""
Task to fetch data from PostgreSQL views using Airflow operators
with declarative connection configuration via AIRFLOW_CONN_*
"""
import logging
from typing import Any

import pandas as pd
from airflow.sdk import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)


def _clean_value(val):
    """Clean JSON-incompatible values (NaN, inf, -inf) -> None"""
    if val is None:
        return None
    if isinstance(val, float):
        if pd.isna(val) or not -1e308 < val < 1e308:  # NaN or inf
            return None
    return val


@task
def fetch_from_postgresql_view(
    view_name: str, postgres_conn_id: str = "postgres_default"
) -> list[dict[str, Any]]:
    """
    Fetch data from a PostgreSQL view using Airflow PostgresHook.

    Uses connection defined in AIRFLOW_CONN_POSTGRES_DEFAULT environment variable.

    Args:
        view_name (str): Name of the PostgreSQL view to query (e.g., 'va_people_crisalid')
        postgres_conn_id (str): Airflow connection ID (defaults to 'postgres_default')

    Returns:
        list[dict]: List of dictionaries where each dict represents a row from the view

    Raises:
        ConnectionError: If unable to connect to PostgreSQL
        ValueError: If view doesn't exist or query fails

    Example:
        >>> data = fetch_from_postgresql_view("va_people_crisalid")
        >>> print(len(data))  # Number of people fetched
    """
    try:
        # Use Airflow PostgresHook with declarative connection (AIRFLOW_CONN_*)
        hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        logger.info("✅ PostgreSQL connection established using %s", postgres_conn_id)

        # Query the view
        query = f"SELECT * FROM {view_name}"
        logger.info("Fetching data from view: %s", view_name)

        # Execute query and get all records
        records = hook.get_records(query)
        logger.info("✅ Fetched %d rows from %s", len(records), view_name)

        # Get column names from description
        cursor = hook.get_conn().cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        cursor.close()

        # Convert to list of dictionaries
        result = [
            {col: _clean_value(val) for col, val in zip(columns, row)}
            for row in records
        ]

        # Log sample data (first row)
        if result:
            logger.debug("Sample row: %s", result[0])

        return result

    except Exception as e:
        logger.error("❌ Error fetching from PostgreSQL view %s: %s", view_name, str(e))
        raise ValueError(f"Failed to fetch from PostgreSQL view '{view_name}': {str(e)}") from e


@task
def fetch_from_postgresql(
    query: str, postgres_conn_id: str = "postgres_default"
) -> list[dict[str, Any]]:
    """
    Fetch data from PostgreSQL using a custom SQL query with Airflow PostgresHook.

    Uses connection defined in AIRFLOW_CONN_POSTGRES_DEFAULT environment variable.

    Args:
        query (str): SQL query to execute
        postgres_conn_id (str): Airflow connection ID (defaults to 'postgres_default')

    Returns:
        list[dict]: List of dictionaries where each dict represents a row

    Raises:
        ConnectionError: If unable to connect to PostgreSQL
        ValueError: If query fails

    Example:
        >>> data = fetch_from_postgresql("SELECT * FROM va_people_crisalid WHERE actif = true")
    """
    try:
        # Use Airflow PostgresHook with declarative connection
        hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        logger.info("✅ PostgreSQL connection established using %s", postgres_conn_id)

        # Execute query
        logger.info("Executing query: %s", query[:100] + "..." if len(query) > 100 else query)
        records = hook.get_records(query)
        logger.info("✅ Query returned %d rows", len(records))

        # Get column names from cursor
        cursor = hook.get_conn().cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        cursor.close()

        # Convert to list of dictionaries
        result = [
            {col: _clean_value(val) for col, val in zip(columns, row)}
            for row in records
        ]

        # Log sample data
        if result:
            logger.debug("Sample row: %s", result[0])

        return result

    except Exception as e:
        logger.error("❌ Error executing PostgreSQL query: %s", str(e))
        raise ValueError(f"Failed to execute PostgreSQL query: {str(e)}") from e
