from test_utils.dags import assert_dag_dict_equal


def test_dag_loaded(dagbag) -> None:
    """
    Test that the DAG has been loaded correctly

    :param dagbag: dagbag fixture
    :return: None
    """
    dag = dagbag.get_dag(dag_id="load_ldap_structures")
    # TODO: Uncomment the next line when import errors are fixed
    # https://www.mail-archive.com/commits@airflow.apache.org/msg416071.html
    # assert dagbag.import_errors == {}
    assert dag is not None
    assert len(dag.tasks) == 12


def test_dag(dagbag) -> None:
    """
    Test that the DAG has the correct structure
    :param dagbag: dagbag fixture
    :return: None
    """
    dag = dagbag.get_dag(dag_id="load_ldap_structures")
    assert_dag_dict_equal(
        {
            "create_redis_connection": ["update_database"],
            "fetch_structures_from_ldap": [
                "structure_fields_conversion_tasks.convert_ldap_structure_long_labels",
                "structure_fields_conversion_tasks.convert_ldap_structure_short_labels",
                "structure_fields_conversion_tasks.convert_ldap_structure_descriptions",
                "structure_fields_conversion_tasks.convert_ldap_structure_contacts",
                "structure_fields_conversion_tasks.convert_ldap_structure_identifiers",
                "structure_fields_conversion_tasks.convert_ldap_structure_type",
                "structure_fields_conversion_tasks.convert_ldap_structure_relationships",
            ],
            "structure_fields_conversion_tasks.convert_ldap_structure_long_labels":
                ["combine_batch_results"],
            "structure_fields_conversion_tasks.convert_ldap_structure_short_labels":
                ["combine_batch_results"],
            "structure_fields_conversion_tasks.convert_ldap_structure_descriptions":
                ["combine_batch_results"],
            "structure_fields_conversion_tasks.convert_ldap_structure_contacts":
                ["combine_batch_results"],
            "structure_fields_conversion_tasks.convert_ldap_structure_identifiers":
                ["combine_batch_results"],
            "structure_fields_conversion_tasks.convert_ldap_structure_type":
                ["combine_batch_results"],
            "structure_fields_conversion_tasks.convert_ldap_structure_relationships":
                ["combine_batch_results"],
            "combine_batch_results": ["update_database"],
            "update_database": ["trigger_broadcast"],
            "trigger_broadcast": [],
        },
        dag,
    )
