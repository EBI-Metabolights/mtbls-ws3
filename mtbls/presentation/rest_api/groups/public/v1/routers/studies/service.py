from logging import getLogger

from mtbls.application.services.interfaces.http_client import HttpClient
from mtbls.domain.entities.http_response import HttpResponse
from mtbls.domain.entities.study import StudyOutput
from mtbls.domain.enums.http_request_type import HttpRequestType
from mtbls.presentation.rest_api.groups.public.v1.routers.studies.schemas import (
    StudyTitle,
)

logger = getLogger(__name__)


async def find_studies_on_europmc_by_orcid(
    http_client: HttpClient, orcid_id: str, submitter_studies: list[StudyOutput]
) -> list[StudyTitle]:
    mtbls_accession_list = [x.accession_number for x in submitter_studies]
    article_id_list = await search_europe_pmc(http_client, orcid=orcid_id)
    if article_id_list:
        for article_id in article_id_list:
            await get_xref(http_client, article_id, mtbls_accession_list)

    mtbls_basic_list: list[StudyTitle] = [
        StudyTitle(accession=x, title=await get_mtbls_title(http_client, acc=x))
        for x in mtbls_accession_list
    ]
    return mtbls_basic_list


async def search_europe_pmc(http_client: HttpClient, orcid: str):
    europe_pmc_url = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
    article_id_list: list[str] = []
    try:
        response: HttpResponse = await http_client.send_request(
            HttpRequestType.GET,
            europe_pmc_url,
            params={"query": orcid, "format": "json"},
            timeout=10,
        )
        response_obj = response.json_data
        if response_obj and response_obj.get("hitCount", 0) > 0:
            result_list = response_obj.get("resultList", {}).get("result", [])
            for result in result_list:
                result_id = result.get("id", None)
                if result_id:
                    article_id_list.append(result["id"])
    except Exception as ex:
        logger.warning("search_europe_pmc error: %s", str(ex))
    return article_id_list


async def get_xref(http_client: HttpClient, arc_id: str, mtbls_acc_list: list[str]):
    europmc_url_prefix = "https://www.ebi.ac.uk/ebisearch/ws/rest/europepmc"
    xref_url = f"{europmc_url_prefix}/entry/{arc_id}/xref/metabolights"
    try:
        response: HttpResponse = await http_client.send_request(
            HttpRequestType.GET, xref_url, params={"format": "json"}, timeout=10
        )
        response_obj = response.json_data

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


async def get_mtbls_title(http_client: HttpClient, acc: str):
    ebi_search_url = "https://www.ebi.ac.uk/ebisearch/ws/rest/metabolights"
    title = ""
    try:
        response: HttpResponse = await http_client.send_request(
            HttpRequestType.GET,
            ebi_search_url,
            params={"query": f"id:{acc}", "fields": "id,name", "format": "json"},
            timeout=10,
        )
        response_obj = response.json_data
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
