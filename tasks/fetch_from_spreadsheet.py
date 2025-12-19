from airflow.decorators import task

from utils.config import get_env_variable
from utils.spreadsheet import read_spreadsheet

FETCH_PARAMETERS = {
    "ldap_people": {
        "path": "PEOPLE_SPREADSHEET_PATH",
        "columns": [
            "local",
            "id_hal_i",
            "id_hal_s",
            "orcid",
            "idref",
            "scopus_eid",
        ]
    },
    "ldap_structures": {
        "path": "",
        "columns": []
    },
    "spreadsheet_people": {
        "path": "PEOPLE_SPREADSHEET_PATH",
        "columns": [
            "first_names",
            "last_name",
            "main_research_structure",
            "tracking_id",
            "eppn",
            "idhal_i",
            "idhal_s",
            "orcid",
            "idref",
            "scopus_eid",
            "institution_identifier",
            "institution_id_nomenclature",
            "position",
            "employment_start_date",
            "employment_end_date",
            "email",
            "other_email",
            "personal_email"
        ]
    },
    "spreadsheet_structures": {
        "path": "STRUCTURE_SPREADSHEET_PATH",
        "columns": [
            "name",
            "acronym",
            "description",
            "tracking_id",
            "nns",
            "ror",
            "city_name",
            "city_code",
            "city_adress",
            "scopus_id",
            "hal_collection",
            "web"
        ]
    }
}


@task(task_id="fetch_from_spreadsheet")
def fetch_from_spreadsheet(entity_source: str, entity_type: str) -> list[dict[str, str]]:
    """
    Fetch the identifiers from a spreadsheet.

    Returns:
        dict: A dict of identifiers with dn as key and a dict with 'identifier'
    """
    key = f"{entity_source}_{entity_type}"
    if key in FETCH_PARAMETERS:
        data_path = get_env_variable(FETCH_PARAMETERS[key]["path"])
        columns_to_read = FETCH_PARAMETERS[key]["columns"]

        df = read_spreadsheet(data_path, columns_to_read)

        rows = df.to_dict(orient='records')

        return rows

    raise KeyError(
        f"The key '{key}' was not found in FETCH_PARAMETERS."
        f" Please check the entity type and source."
    )
