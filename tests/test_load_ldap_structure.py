from test_utils.dags import assert_dag_dict_equal


def test_dag_loaded(dagbag) -> None:
    """
    Test that the DAG has been loaded correctly

    :param dagbag: dagbag fixture
    :return: None
    """
    dag = dagbag.get_dag(dag_id="load_ldap_structures")
    assert dagbag.import_errors == {}
    print("******Debug github actions********")
    print(dagbag.import_errors)
    assert dag is not None
    assert len(dag.tasks) == 7


def test_dag(dagbag) -> None:
    """
    Test that the DAG has the correct structure
    :param dagbag: dagbag fixture
    :return: None
    """
    dag = dagbag.get_dag(dag_id="load_ldap_structures")
    assert_dag_dict_equal(
        {
            "fetch_structures_task": [
                "conversion_tasks.convert_ldap_structure_name_task",
                "conversion_tasks.convert_ldap_structure_acronym_task",
                "conversion_tasks.convert_ldap_structure_description_task",
                "conversion_tasks.convert_ldap_structure_address_task",
            ],
            "conversion_tasks.convert_ldap_structure_name_task": ["combine_results"],
            "conversion_tasks.convert_ldap_structure_acronym_task": ["combine_results"],
            "conversion_tasks.convert_ldap_structure_description_task": ["combine_results"],
            "conversion_tasks.convert_ldap_structure_address_task": ["combine_results"],
            "combine_results": ["update_database_task"],
            "update_database_task": [],
        },
        dag,
    )
