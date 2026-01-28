import logging

from fastmcp import FastMCP

from mtbls.run.config_utils import get_application_config_files
from mtbls.run.rest_api.submission.containers import Ws3ApplicationContainer
from mtbls.run.rest_api.submission.main import get_app

logger = logging.getLogger(__name__)


if __name__ == "__main__":
    app_container: Ws3ApplicationContainer = Ws3ApplicationContainer()
    config_file_path, secrets_file_path = get_application_config_files()
    fast_app = get_app(config_file_path, secrets_file_path, app_container)
    mcp = FastMCP.from_fastapi(app=fast_app)
    mcp.run(transport="http")
