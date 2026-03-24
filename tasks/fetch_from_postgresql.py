"""
Task to fetch data from PostgreSQL views
"""
import logging
from typing import Any

import pandas as pd
from sqlalchemy import create_engine, text
from airflow.sdk import task

from utils.config import get_postgresql_connection_string

logger = logging.getLogger(__name__)


@task
def fetch_from_postgresql_view(view_name: str) -> list[dict[str, Any]]:
    """
    Fetch data from a PostgreSQL view and return as list of dictionaries.

    Args:
        view_name (str): Name of the PostgreSQL view to query (e.g., 'va_people_crisalid')

    Returns:
        list[dict]: List of dictionaries where each dict represents a row from the view

    Raises:
        ConnectionError: If unable to connect to PostgreSQL
        ValueError: If view doesn't exist or query fails

    Example:
        >>> data = fetch_from_postgresql_view("va_people_crisalid")
        >>> print(len(data))  # Number of people fetched
    """
    connection_string = None
    engine = None
    try:
        # Get connection string from environment
        connection_string = get_postgresql_connection_string()
        logger.info("Connecting to PostgreSQL: %s", connection_string.split("@")[1])

        # Create SQLAlchemy engine
        engine = create_engine(connection_string)

        # Validate connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            logger.info("✅ PostgreSQL connection successful")

        # Query the view
        query = f"SELECT * FROM {view_name}"
        logger.info("Fetching data from view: %s", view_name)

        df = pd.read_sql(query, engine)
        logger.info("✅ Fetched %d rows from %s", len(df), view_name)

        # Convert DataFrame to list of dictionaries
        result = df.to_dict('records')

        # Clean JSON-incompatible values (NaN, inf, -inf) -> None
        def clean_value(val):
            if val is None:
                return None
            if isinstance(val, float):
                if pd.isna(val) or not -1e308 < val < 1e308:  # NaN or inf
                    return None
            return val

        result = [
            {k: clean_value(v) for k, v in row.items()}
            for row in result
        ]

        # Log sample data (first row)
        if result:
            logger.debug("Sample row: %s", result[0])

        return result

    except Exception as e:
        logger.error("❌ Error fetching from PostgreSQL view %s: %s", view_name, str(e))
        raise ValueError(f"Failed to fetch from PostgreSQL view '{view_name}': {str(e)}") from e
    finally:
        # Properly dispose of the engine to close all connections
        if engine is not None:
            engine.dispose()
            logger.debug("PostgreSQL engine disposed")


@task
def fetch_from_postgresql(query: str) -> list[dict[str, Any]]:
    """
    Fetch data from PostgreSQL using a custom SQL query.

    Args:
        query (str): SQL query to execute

    Returns:
        list[dict]: List of dictionaries where each dict represents a row

    Raises:
        ConnectionError: If unable to connect to PostgreSQL
        ValueError: If query fails

    Example:
        >>> data = fetch_from_postgresql("SELECT * FROM va_people_crisalid WHERE actif = true")
    """
    try:
        # Get connection string from environment
        connection_string = get_postgresql_connection_string()
        logger.info("Connecting to PostgreSQL")

        # Create SQLAlchemy engine
        engine = create_engine(connection_string)

        # Execute query
        logger.info("Executing query: %s", query[:100] + "..." if len(query) > 100 else query)
        df = pd.read_sql(query, engine)
        logger.info("✅ Query returned %d rows", len(df))

        # Convert to list of dictionaries
        result = df.to_dict('records')

        # Clean JSON-incompatible values (NaN, inf, -inf) -> None
        def clean_value(val):
            if val is None:
                return None
            if isinstance(val, float):
                if pd.isna(val) or not -1e308 < val < 1e308:  # NaN or inf
                    return None
            return val

        result = [
            {k: clean_value(v) for k, v in row.items()}
            for row in result
        ]

        # Log sample data
        if result:
            logger.debug("Sample row: %s", result[0])

        return result

    except Exception as e:
        logger.error("❌ Error executing PostgreSQL query: %s", str(e))
        raise ValueError(f"Failed to execute PostgreSQL query: {str(e)}") from e
    finally:
        # Properly dispose of the engine to close all connections
        if engine is not None:
            engine.dispose()
            logger.debug("PostgreSQL engine disposed")
        raise ValueError(f"Failed to execute PostgreSQL query: {str(e)}") from e
