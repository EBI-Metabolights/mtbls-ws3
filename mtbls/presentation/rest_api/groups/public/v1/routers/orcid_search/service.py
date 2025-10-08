from logging import getLogger
from typing import Any

from mtbls.application.services.interfaces.http_client import HttpClient
from mtbls.application.services.interfaces.repositories.study.study_read_repository import (  # noqa E501
    StudyReadRepository,
)
from mtbls.domain.entities.http_response import HttpResponse
from mtbls.domain.entities.study import StudyOutput
from mtbls.domain.enums.http_request_type import HttpRequestType
from mtbls.domain.enums.study_status import StudyStatus
from mtbls.presentation.rest_api.groups.public.v1.routers.orcid_search.schemas import (
    Citation,
    CitedDataset,
    MetaboLightsStudyCitation,
)

logger = getLogger(__name__)


async def find_studies_on_europmc_by_orcid(
    http_client: HttpClient,
    study_read_repository: StudyReadRepository,
    orcid_id: str,
    submitter_public_studies: list[StudyOutput],
) -> list[MetaboLightsStudyCitation]:
    submitter_all_studies = {x.accession_number: x for x in submitter_public_studies}
    submitter_public_studies: dict[str, MetaboLightsStudyCitation] = {
        x.accession_number: MetaboLightsStudyCitation(
            study_accession=x.accession_number, is_submitter=True, is_public=True
        )
        for x in submitter_public_studies
        if x.status == StudyStatus.PUBLIC
    }

    async def find_study_status(mtbls_id: str) -> StudyStatus:
        if mtbls_id in submitter_all_studies:
            return submitter_all_studies[mtbls_id].status
        study = await study_read_repository.get_study_by_accession(mtbls_id)
        return study.status

    mtbls_studies: dict[str, MetaboLightsStudyCitation] = (
        submitter_public_studies.copy()
    )

    articles = await search_europe_pmc(http_client, orcid=orcid_id)

    article_id_mtbls_id_map = await find_mtbls_ids_for_article_ids(
        http_client, list(articles.keys())
    )

    mtbls_id_article_id_map: dict[str, set[str]] = {}
    for article_id, mtbls_id_list in article_id_mtbls_id_map.items():
        for mtbls_id in mtbls_id_list:
            if mtbls_id not in mtbls_id_article_id_map:
                mtbls_id_article_id_map[mtbls_id] = set()
            mtbls_id_article_id_map[mtbls_id].add(article_id)
    all_mtbls_ids = set(mtbls_studies.keys())

    all_mtbls_ids.update(mtbls_id_article_id_map.keys())

    mtbls_study_titles = {
        x: await get_mtbls_title(http_client, mtbls_id=x) for x in all_mtbls_ids
    }

    for mtbls_id in all_mtbls_ids:
        article_ids = mtbls_id_article_id_map.get(mtbls_id, [])
        article_ids = mtbls_id_article_id_map.get(mtbls_id, [])
        if mtbls_id not in mtbls_studies:
            mtbls_studies[mtbls_id] = MetaboLightsStudyCitation(
                study_accession=mtbls_id,
                is_submitter=False,
                is_public=await find_study_status(mtbls_id) == StudyStatus.PUBLIC,
            )
        mtbls_studies[mtbls_id].study_title = mtbls_study_titles.get(mtbls_id)
        mtbls_studies[mtbls_id].publications = [
            Citation(
                title=articles[x].get("title", ""),
                doi=articles[x].get("doi", ""),
                pubmed_id=articles[x].get("pmid", ""),
                authors=articles[x].get("authorString", ""),
                journal=articles[x].get("journalTitle", ""),
                publication_date=articles[x].get("firstPublicationDate", ""),
                cited_datasets=[
                    CitedDataset(
                        study_accession=x,
                        is_submitter=submitter_all_studies.get(x, None) is not None,
                        is_public=await find_study_status(x) == StudyStatus.PUBLIC,
                    )
                    for x in article_id_mtbls_id_map.get(x)
                ]
                or None,
            )
            for x in article_ids
        ]
        mtbls_studies[mtbls_id].publications.sort(
            key=lambda x: x.publication_date, reverse=True
        )
    result = list(mtbls_studies.values())
    result.sort(key=lambda x: int(x.study_accession.replace("MTBLS", "")), reverse=True)
    return result


async def search_europe_pmc(
    http_client: HttpClient, orcid: str
) -> dict[str, dict[str, Any]]:
    europe_pmc_url = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
    article_ids: dict[str, dict[str, Any]] = {}
    try:
        response: HttpResponse = await http_client.send_request(
            HttpRequestType.GET,
            europe_pmc_url,
            params={"query": f"AUTHORID:{orcid}", "format": "json", "pageSize": 250},
            timeout=10,
        )
        response_obj = response.json_data
        if response_obj and response_obj.get("hitCount", 0) > 0:
            result_list = response_obj.get("resultList", {}).get("result", [])
            for result in result_list:
                result_id = result.get("id", None)
                if result_id:
                    article_ids[result_id] = result
    except Exception as ex:
        logger.warning("search_europe_pmc error: %s", str(ex))
    return article_ids


async def find_mtbls_ids_for_article_ids(
    http_client: HttpClient, article_ids: list[str]
) -> dict[str, list[str]]:
    mtbls_study_references: dict[str, list[str]] = {}
    europmc_url_prefix = "https://www.ebi.ac.uk/ebisearch/ws/rest/europepmc"
    for arc_id in article_ids:
        xref_url = f"{europmc_url_prefix}/entry/{arc_id}/xref/metabolights"
        try:
            response: HttpResponse = await http_client.send_request(
                HttpRequestType.GET, xref_url, params={"format": "json"}, timeout=10
            )
            response_obj = response.json_data

            if response_obj and response_obj.get("entries", []):
                entries = response_obj.get("entries", [])
                if not entries:
                    continue
                entry = entries[0]
                if not isinstance(entry, dict):
                    continue
                mtbls_study_references[arc_id] = []
                references_list = entry.get("references", [])
                for reference in references_list:
                    reference_id = reference.get("id")
                    if not reference_id or not reference_id.startswith("MTBLS"):
                        continue
                    mtbls_study_references[arc_id].append(reference_id)
        except Exception as ex:
            logger.warning("search xref in ebisearch ended in error: %s", str(ex))
    return mtbls_study_references


async def get_mtbls_title(http_client: HttpClient, mtbls_id: str):
    ebi_search_url = "https://www.ebi.ac.uk/ebisearch/ws/rest/metabolights"
    title = ""
    try:
        response: HttpResponse = await http_client.send_request(
            HttpRequestType.GET,
            ebi_search_url,
            params={"query": f"id:{mtbls_id}", "fields": "id,name", "format": "json"},
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
