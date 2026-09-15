import json

import pytest
from airflow.utils.state import TaskInstanceState

from test_utils.dags import create_dag_run, \
    create_task_instance, \
    DATA_INTERVAL_START, \
    DATA_INTERVAL_END

TEST_TASK_ID = "convert_spreadsheet_structures"

TESTED_TASK_NAME = 'tasks.spreadsheet.convert_spreadsheet_structures.convert_spreadsheet_structures'


@pytest.mark.parametrize("dag", [
    {
        "task_name": TESTED_TASK_NAME,
        "param_names": ["raw_results"],
        "raw_results": [
                    {
                        'generic_type': 'institution',
                        'type': None,
                        'local_id': 'SU',
                        'short_labels': 'Sorbonne Université[fr]',
                        'long_labels': 'Sorbonne Université[fr]',
                        'descriptions': 'Sorbonne Université est une université française multidisciplinaire[fr]',
                        'inclusions': '',
                        'participations': '',
                        'uai': '0750973U',
                        'nns': '130023385',
                        'ror': '02en5vm52',
                        'isni': '',
                        'wikidata': '',
                        'scopus': '',
                        'erc_research_field': '',
                        'hceres_research_areas': '',
                        'hal_collection': '',
                        'web': 'https://www.sorbonne-universite.fr/',
                        'signature': 'Sorbonne Université',
                        'campus': '',
                        'main_mission': 'research',
                        'secondary_missions': 'teaching|scientific_services',
                        'local_types': ''
                    },
                    {
                        'generic_type': 'institution',
                        'type': None,
                        'local_id': 'INSERM',
                        'short_labels': 'INSERM[fr]',
                        'long_labels': 'INSERM[fr]',
                        'descriptions': 'Institut National de la Santé et de la Recherche Médicale[fr]',
                        'inclusions': '',
                        'participations': '',
                        'uai': '',
                        'nns': '',
                        'ror': '00z0af360',
                        'isni': '',
                        'wikidata': '',
                        'scopus': '',
                        'erc_research_field': '',
                        'hceres_research_areas': '',
                        'hal_collection': '',
                        'web': 'https://www.inserm.fr/',
                        'signature': 'INSERM',
                        'campus': '',
                        'main_mission': 'research',
                        'secondary_missions': 'scientific_services',
                        'local_types': ''
                    },
                    {
                        'generic_type': 'institution_subdivision',
                        'type': 'FAC',
                        'local_id': 'FAC_SCI',
                        'short_labels': 'Fac Sciences[fr]',
                        'long_labels': 'Faculté des Sciences[fr]',
                        'descriptions': 'Faculté des Sciences de Sorbonne Université[fr]',
                        'inclusions': 'SU[20100101-20301231]',
                        'participations': '',
                        'uai': '',
                        'nns': '',
                        'ror': '',
                        'isni': '',
                        'wikidata': '',
                        'scopus': '',
                        'erc_research_field': '',
                        'hceres_research_areas': '',
                        'hal_collection': '',
                        'web': '',
                        'signature': '',
                        'campus': '',
                        'main_mission': 'research',
                        'secondary_missions': 'teaching',
                        'local_types': ''
                    },
                    {
                        'generic_type': 'institution_subdivision',
                        'type': 'UFR',
                        'local_id': 'UFR_MED',
                        'short_labels': 'UFR Médecine[fr]',
                        'long_labels': 'UFR de Médecine[fr]',
                        'descriptions': 'Unité de Formation et de Recherche en Médecine[fr]',
                        'inclusions': 'FAC_SCI[20100101-]',
                        'participations': '',
                        'uai': '',
                        'nns': '',
                        'ror': '',
                        'isni': '',
                        'wikidata': '',
                        'scopus': '',
                        'erc_research_field': '',
                        'hceres_research_areas': '',
                        'hal_collection': '',
                        'web': '',
                        'signature': '',
                        'campus': '',
                        'main_mission': 'research',
                        'secondary_missions': 'teaching',
                        'local_types': ''
                    },
                    {
                        'generic_type': 'institution_subdivision',
                        'type': 'UFR',
                        'local_id': 'CASU',
                        'short_labels': 'CASU[fr]',
                        'long_labels': 'Centre d\'Anatomie et de Stérilité de l\'Université[fr]',
                        'descriptions': 'Centre d\'Anatomie et de Stérilité de l\'Université - Sorbonne Université[fr]',
                        'inclusions': 'SU[]',
                        'participations': '',
                        'uai': '',
                        'nns': '',
                        'ror': '',
                        'isni': '',
                        'wikidata': '',
                        'scopus': '',
                        'erc_research_field': '',
                        'hceres_research_areas': '',
                        'hal_collection': '',
                        'web': '',
                        'signature': '',
                        'campus': '',
                        'main_mission': 'research',
                        'secondary_missions': '',
                        'local_types': ''
                    },
                    {
                        'generic_type': 'unit',
                        'type': 'USM',
                        'local_id': 'SUMMIT',
                        'short_labels': 'SUMMIT[fr]',
                        'long_labels': 'SUMMIT[fr]',
                        'descriptions': 'Unité de Service et de Micro-technologie en Imagerie et Transformation[fr]',
                        'inclusions': 'CASU[20150601-20251231]',
                        'participations': '',
                        'uai': '',
                        'nns': '',
                        'ror': '',
                        'isni': '',
                        'wikidata': '',
                        'scopus': '',
                        'erc_research_field': '',
                        'hceres_research_areas': '',
                        'hal_collection': '',
                        'web': '',
                        'signature': '',
                        'campus': '',
                        'main_mission': 'research',
                        'secondary_missions': '',
                        'local_types': ''
                    },
                    {
                        'generic_type': 'unit',
                        'type': 'PLATEFORME',
                        'local_id': 'PLAT_XYZ',
                        'short_labels': 'Plat XYZ[fr]',
                        'long_labels': 'Plateforme XYZ[fr]',
                        'descriptions': 'Plateforme de recherche XYZ - Sorbonne Université[fr]',
                        'inclusions': 'UFR_MED[]',
                        'participations': '',
                        'uai': '',
                        'nns': '',
                        'ror': '',
                        'isni': '',
                        'wikidata': '',
                        'scopus': '',
                        'erc_research_field': '',
                        'hceres_research_areas': '',
                        'hal_collection': '',
                        'web': '',
                        'signature': '',
                        'campus': '',
                        'main_mission': 'research',
                        'secondary_missions': '',
                        'local_types': 'plateforme'
                    },
                    {
                        'generic_type': 'unit',
                        'type': 'UMR',
                        'local_id': 'ABC_UNIT',
                        'short_labels': 'ABC[fr]',
                        'long_labels': 'ABC Unit[fr]',
                        'descriptions': 'ABC Unit de recherche[fr]',
                        'inclusions': 'PLAT_XYZ[]',
                        'participations': '',
                        'uai': '',
                        'nns': '',
                        'ror': '',
                        'isni': '',
                        'wikidata': '',
                        'scopus': '',
                        'erc_research_field': '',
                        'hceres_research_areas': '',
                        'hal_collection': '',
                        'web': '',
                        'signature': '',
                        'campus': '',
                        'main_mission': 'research',
                        'secondary_missions': '',
                        'local_types': ''
                    },
                    {
                        'generic_type': 'unit',
                        'type': 'UMR',
                        'local_id': 'PATHO_UNIT',
                        'short_labels': 'Pathologie[fr]',
                        'long_labels': 'Pathologie[fr]',
                        'descriptions': 'Unité de Recherche en Pathologie - UMR/INSERM/CNRS[fr]',
                        'inclusions': 'PLAT_XYZ[]|UFR_MED[]',
                        'participations': 'SU[main_supervision]|INSERM[associated_supervision]',
                        'uai': '',
                        'nns': '000123456',
                        'ror': '',
                        'isni': '',
                        'wikidata': '',
                        'scopus': '',
                        'erc_research_field': '',
                        'hceres_research_areas': '',
                        'hal_collection': '',
                        'web': '',
                        'signature': '',
                        'campus': '',
                        'main_mission': 'research',
                        'secondary_missions': '',
                        'local_types': ''
                    },
                    {
                        'generic_type': 'team',
                        'type': 'TEAM',
                        'local_id': 'EQUIPE_PATHO',
                        'short_labels': 'Équipe Pathologie[fr]',
                        'long_labels': 'Équipe de Pathologie[fr]',
                        'descriptions': 'Équipe de recherche en pathologie[fr]',
                        'inclusions': 'PATHO_UNIT[]',
                        'participations': '',
                        'uai': '',
                        'nns': '',
                        'ror': '',
                        'isni': '',
                        'wikidata': '',
                        'scopus': '',
                        'erc_research_field': '',
                        'hceres_research_areas': '',
                        'hal_collection': '',
                        'web': '',
                        'signature': '',
                        'campus': '',
                        'main_mission': 'research',
                        'secondary_missions': '',
                        'local_types': ''
                    }
                ]
            }
], indirect=['dag'])
def test_convert_spreadsheet_structures(dag, unique_logical_date) -> None:
    """
    Test that the csv data are converted to the expected format with complete hierarchy
    """
    dag_run = create_dag_run(dag, DATA_INTERVAL_START, DATA_INTERVAL_END, unique_logical_date)
    ti = create_task_instance(dag, dag_run, TEST_TASK_ID)
    ti.run(ignore_ti_state=True)

    assert ti.state == TaskInstanceState.SUCCESS
    result = ti.xcom_pull(task_ids=TEST_TASK_ID)

    # Result should be a dict with 10 structures (keyed by local_id)
    assert len(result) == 10
    
    # Load expected data from individual structure files
    structure_ids = ['su', 'inserm', 'fac_sci', 'ufr_med', 'casu', 'summit', 'plat_xyz', 'abc_unit', 'patho_unit', 'equipe_patho']
    expected_structures = {}
    
    for structure_id in structure_ids:
        file_path = f"./tests/data/structures/{structure_id}.json"
        with open(file_path, 'r', encoding='utf-8') as f:
            structure_event = json.load(f)
            expected_structures[structure_id.upper()] = structure_event['structures_event']['data']
    
    # Compare each structure
    assert result == expected_structures
