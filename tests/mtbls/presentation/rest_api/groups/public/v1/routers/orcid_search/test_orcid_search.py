from fastapi import status
from fastapi.testclient import TestClient

from mtbls.application.services.interfaces.http_client import HttpClient
from mtbls.domain.entities.http_response import HttpResponse
from mtbls.presentation.rest_api.core.responses import APIResponse
from mtbls.presentation.rest_api.groups.public.v1.routers.orcid_search.schemas import (
    MetaboLightsStudyCitation,
)


def test_get_studies_by_orcid_id(
    public_api_client: TestClient, submission_api_container
):
    orcid = "0000-0002-7899-7192"
    url = f"/public/v2/orcids/{orcid}/studies"

    def send_request_side_effects(*args, **kwargs):
        url = args[1]
        europmc_orcid_search_result = {
            "hitCount": 29,
            "resultList": {
                "result": [
                    {
                        "id": "30152810",
                        "pmid": "30152810",
                        "doi": "10.1038/sdata.2018.179",
                        "title": "Computational workflow to study the seasonal "
                        "variation of secondary metabolites "
                        "in nine different bryophytes.",
                        "firstPublicationDate": "2018-08-28",
                    },
                ]
            },
        }
        xref_search_result = {
            "entries": [
                {
                    "id": "30152810",
                    "source": "europepmc",
                    "referenceCount": 1,
                    "references": [{"id": "MTBLS800003", "source": "metabolights"}],
                }
            ]
        }
        mtbls_id_search_result = {
            "hitCount": 1,
            "entries": [
                {
                    "id": "MTBLS800003",
                    "source": "metabolights",
                    "fields": {
                        "id": ["MTBLS800003"],
                        "name": [
                            "Computational workflow to study the seasonal variation "
                            "of secondary metabolites in nine different bryophytes"
                        ],
                    },
                }
            ],
        }

        if url.startswith("https://www.ebi.ac.uk/europepmc/webservices/rest/search"):
            return HttpResponse(status_code=200, json_data=europmc_orcid_search_result)
        elif url.startswith("https://www.ebi.ac.uk/ebisearch/ws/rest/europepmc/entry"):
            return HttpResponse(status_code=200, json_data=xref_search_result)
        elif url.startswith("https://www.ebi.ac.uk/ebisearch/ws/rest/metabolights"):
            return HttpResponse(status_code=200, json_data=mtbls_id_search_result)
        return HttpResponse(status_code=500, error=True)

    http_client: HttpClient = submission_api_container.gateways.http_client()
    http_client.send_request.side_effect = send_request_side_effects
    response = public_api_client.get(url)
    assert response.status_code == status.HTTP_200_OK
    json_data = response.json()
    assert json_data
    result = APIResponse[MetaboLightsStudyCitation].model_validate(response.json())
    # check local/sqlite/initial_data.sql for initial values
    assert result
    assert result.content
