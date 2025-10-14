import asyncio
import logging

from fastmcp import Client, settings

settings.experimental.enable_new_openapi_parser = True

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main():
    async with Client("http://localhost:8000/mcp") as client:
        tools = await client.list_tools()
        for idx, tool in enumerate(tools, start=1):
            logger.info("Tool %d\t: %s", idx, tool.name)
        tool_name = "get_status_system_v2_transfer_status_get"
        logger.info("call: %s", tool_name)
        result = await client.call_tool(tool_name)
        logger.info("Result: %s", result.data)


asyncio.run(main())
