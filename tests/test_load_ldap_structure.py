from test_utils.dags import assert_dag_dict_equal


def test_dag_loaded(dagbag) -> None:
    """
    Test that the DAG has been loaded correctly

    :param dagbag: dagbag fixture
    :return: None
    """
    dag = dagbag.get_dag(dag_id="load_ldap_structures")
    assert dagbag.import_errors == {}
    assert dag is not None
    assert len(dag.tasks) == 10


def test_dag(dagbag) -> None:
    """
    Test that the DAG has the correct structure
    :param dagbag: dagbag fixture
    :return: None
    """
    dag = dagbag.get_dag(dag_id="load_ldap_structures")
    assert_dag_dict_equal(
        {
            "create_redis_connection_task": ["update_database"],
            "fetch_structures_task": [
                "structure_fields_conversion_tasks.convert_ldap_structure_names",
                "structure_fields_conversion_tasks.convert_ldap_structure_acronyms",
                "structure_fields_conversion_tasks.convert_ldap_structure_descriptions",
                "structure_fields_conversion_tasks.convert_ldap_structure_contacts",
                "structure_fields_conversion_tasks.convert_ldap_structure_identifiers",
            ],
            "structure_fields_conversion_tasks.convert_ldap_structure_identifiers":
                ["combine_batch_results"],
            "structure_fields_conversion_tasks.convert_ldap_structure_names":
                ["combine_batch_results"],
            "structure_fields_conversion_tasks.convert_ldap_structure_acronyms":
                ["combine_batch_results"],
            "structure_fields_conversion_tasks.convert_ldap_structure_descriptions":
                ["combine_batch_results"],
            "structure_fields_conversion_tasks.convert_ldap_structure_contacts":
                ["combine_batch_results"],
            "combine_batch_results": ["update_database"],
            "update_database": ["trigger_broadcast"],
            "trigger_broadcast": [],
        },
        dag,
    )
