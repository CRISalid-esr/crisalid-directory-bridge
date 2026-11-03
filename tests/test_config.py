import pytest

from utils import config
from utils.config import get_env_variable


@pytest.fixture(name="airflow_variables")
def airflow_variables_fixture(monkeypatch) -> dict:
    """
    Replace Airflow Variable.get by an in-memory dict lookup.
    """
    variables = {}

    def fake_get(key, default=None, deserialize_json=False):  # pylint: disable=unused-argument
        return variables.get(key, default)

    monkeypatch.setattr("airflow.sdk.Variable.get", fake_get)
    return variables


def test_airflow_variable_overrides_environment(monkeypatch, airflow_variables):
    monkeypatch.setenv("STRUCTURE_SPREADSHEET_PATH", "./from_env.csv")
    airflow_variables["STRUCTURE_SPREADSHEET_PATH"] = "./from_variable.csv"
    assert get_env_variable("STRUCTURE_SPREADSHEET_PATH") == "./from_variable.csv"


def test_environment_used_when_no_airflow_variable(monkeypatch, airflow_variables):
    monkeypatch.setenv("STRUCTURE_SPREADSHEET_PATH", "./from_env.csv")
    assert get_env_variable("STRUCTURE_SPREADSHEET_PATH") == "./from_env.csv"


def test_empty_airflow_variable_is_ignored(monkeypatch, airflow_variables):
    monkeypatch.setenv("PEOPLE_SPREADSHEET_PATH", "./from_env.csv")
    airflow_variables["PEOPLE_SPREADSHEET_PATH"] = ""
    assert get_env_variable("PEOPLE_SPREADSHEET_PATH") == "./from_env.csv"


def test_non_overridable_names_ignore_airflow_variables(monkeypatch, airflow_variables):
    monkeypatch.setenv("LDAP_DEFAULT_LANGUAGE", "fr")
    airflow_variables["LDAP_DEFAULT_LANGUAGE"] = "en"
    assert get_env_variable("LDAP_DEFAULT_LANGUAGE") == "fr"


def test_airflow_variable_lookup_failure_falls_back(monkeypatch):
    def failing_get(*args, **kwargs):
        raise RuntimeError("no execution API")

    monkeypatch.setattr("airflow.sdk.Variable.get", failing_get)
    monkeypatch.setenv("STRUCTURE_SPREADSHEET_PATH", "./from_env.csv")
    assert get_env_variable("STRUCTURE_SPREADSHEET_PATH") == "./from_env.csv"


def test_missing_everywhere_raises(monkeypatch, airflow_variables):
    monkeypatch.delenv("STRUCTURE_SPREADSHEET_PATH", raising=False)
    with pytest.raises(ValueError):
        get_env_variable("STRUCTURE_SPREADSHEET_PATH")


def test_real_variable_get_outside_task_context_returns_none():
    """
    Outside a worker, the SDK must report "not found" without raising,
    so the environment fallback is reached.
    """
    assert config._get_airflow_variable("STRUCTURE_SPREADSHEET_PATH") is None  # pylint: disable=protected-access
