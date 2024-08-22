import logging

from airflow.decorators import task

from utils.list import ensure_list

logger = logging.getLogger(__name__)


@task(task_id="convert_ldap_people_memberships")
def convert_ldap_people_memberships(ldap_results: dict[str, dict[str, str | dict]]) \
        -> dict[str, dict[str, str | dict]]:
    """Extract the 'supannentiteaffectation' field from an LDAP entry and process all affectations.

    Args:
        ldap_results (dict): A dict of ldap results with dn as key and entry as value

    Returns:
        dict: A dict of converted results with the "memberships" field populated
    """
    task_results = {}
    for dn, entry in ldap_results.items():
        affectations = []
        main_affectation = entry.get('supannEntiteAffectationPrincipale', None)
        if isinstance(main_affectation, list) and len(main_affectation) > 0:
            affectations.append(main_affectation[0])
        other_affectations = ensure_list(entry.get('supannEntiteAffectation', []))
        for affectation in other_affectations:
            if affectation in affectations or affectation == main_affectation:
                continue
            affectations.append(affectation)
        task_results[dn] = {"memberships": [{
            "entity": affectation
        } for affectation in affectations]}
    return task_results
