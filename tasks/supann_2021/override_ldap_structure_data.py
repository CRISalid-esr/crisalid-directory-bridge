import logging

from airflow.sdk import task

logger = logging.getLogger(__name__)


@task(task_id="override_ldap_structure_data")
def override_ldap_structure_data(ldap_source: dict, spreadsheet_source: dict) -> dict:
    """
    Merge spreadsheet structure records into LDAP-sourced records.

    Merge rules:
    - LDAP-only entry: kept as-is.
    - Both LDAP and CSV: field-by-field merge; non-falsy CSV values overwrite LDAP values.
    - CSV-only entry: added to the result (expected for structures absent from LDAP).

    Args:
        ldap_source (dict): Combined records from LDAP, keyed by supannCodeEntite.
        spreadsheet_source (dict): Converted spreadsheet records, keyed by local_id.

    Returns:
        dict: Merged records keyed by entity code.
    """
    result = {key: dict(value) for key, value in ldap_source.items()}

    for key, csv_record in spreadsheet_source.items():
        if key in result:
            for field, value in csv_record.items():
                if value:
                    result[key][field] = value
        else:
            logger.info("CSV-only structure '%s' added to results", key)
            result[key] = csv_record

    return result
