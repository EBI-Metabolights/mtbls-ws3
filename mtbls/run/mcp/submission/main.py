import asyncio
import logging

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastmcp import FastMCP

from mtbls.presentation.rest_api.core.models import ApiServerConfiguration
from mtbls.run.config_utils import get_application_config_files
from mtbls.run.rest_api.submission.containers import Ws3ApplicationContainer
from mtbls.run.rest_api.submission.main import get_app

logger = logging.getLogger(__name__)


async def health_check(request):
    return JSONResponse({"status": "healthy", "service": "mcp-server"})


async def start_mcp_server(config_file_path: str, secrets_file_path: str):
    app_container: Ws3ApplicationContainer = Ws3ApplicationContainer()

    fast_app: FastAPI = await get_app(
        config_file_path, secrets_file_path, app_container
    )
    mcp = FastMCP.from_fastapi(app=fast_app)
    server_configuration: ApiServerConfiguration = app_container.api_server_config()
    config = server_configuration.server_info
    root_path = config.root_path.rstrip("/") if config.root_path else ""
    if root_path:
        parts = root_path.split("/")
        if len(parts) > 0:
            root_path = "/".join(parts[:-1])

    await mcp.run_async(
        transport="http",
        host="0.0.0.0",
        port=int(server_configuration.port),
        path=f"{root_path}/mcp/",
    )


if __name__ == "__main__":
    config_file_path, secrets_file_path = get_application_config_files()
    asyncio.run(start_mcp_server(config_file_path, secrets_file_path))
