import json
from logging import getLogger

import httpx

from mtbls.domain.entities.study import StudyOutput
from mtbls.presentation.rest_api.groups.public.v1.routers.studies.schemas import (
    StudyTitle,
)

logger = getLogger(__name__)


async def find_studies_on_europmc_by_orcid(
    orcid_id: str, submitter_studies: list[StudyOutput]
) -> list[StudyTitle]:
    mtbls_accession_list = [x.accession_number for x in submitter_studies]
    article_id_list = search_europe_pmc(orcid=orcid_id)
    if article_id_list:
        for article_id in article_id_list:
            get_xref(article_id, mtbls_accession_list)

    mtbls_basic_list: list[StudyTitle] = [
        StudyTitle(accession=x, title=get_mtbls_title(acc=x))
        for x in mtbls_accession_list
    ]
    return mtbls_basic_list


def search_europe_pmc(orcid: str):
    europe_pmc_url = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
    article_id_list: list[str] = []
    try:
        response = httpx.get(
            europe_pmc_url,
            headers={"query": orcid, "format": "json"},
            timeout=10,
        )
        response_obj = json.loads(response.text)
        if response_obj and response_obj["hitCount"] > 0:
            result_list = response_obj["resultList"]["result"]
            for result in result_list:
                article_id_list.append(result["id"])
    except Exception as ex:
        logger.warning("search_europe_pmc error: %s", str(ex))
    return article_id_list


def get_xref(arc_id: str, mtbls_acc_list: list[str]):
    europmc_url_prefix = "https://www.ebi.ac.uk/ebisearch/ws/rest/europepmc"
    xref_url = f"{europmc_url_prefix}/entry/{arc_id}/xref/metabolights"
    try:
        response = httpx.get(xref_url, headers={"format": "json"}, timeout=10)
        response_obj = json.loads(response.text)

        if response_obj and response_obj.get("entries", []):
            entries = response_obj.get("entries", [])
            entries_0 = entries[0]
            references_list = entries_0["references"]
            for reference in references_list:
                if reference["id"] not in mtbls_acc_list:
                    mtbls_acc_list.append(reference["id"])
    except Exception as ex:
        logger.warning("search xref in ebisearch ended in error: %s", str(ex))
    return mtbls_acc_list


def get_mtbls_title(acc: str):
    ebi_search_url = "https://www.ebi.ac.uk/ebisearch/ws/rest/metabolights"
    title = ""
    try:
        response = httpx.get(
            ebi_search_url,
            headers={"query": f"id:{acc}", "fields": "id,name", "format": "json"},
            timeout=10,
        )
        response_obj = json.loads(response.text)
        if response_obj and response_obj["hitCount"] > 0:
            if response_obj["entries"]:
                entries = response_obj["entries"]
                entries_0 = entries[0]
                fields = entries_0["fields"]
                name = fields["name"]
                title = name[0]
    except Exception as ex:
        logger.warning("search xref in ebisearch ended in error: %s", str(ex))
    return title
