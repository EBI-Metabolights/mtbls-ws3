import asyncio
import logging
from pathlib import Path

from sqlalchemy import text

from mtbls.infrastructure.persistence.db.model.study_models import Base
from mtbls.infrastructure.persistence.db.sqlite.config import SQLiteDatabaseConnection
from mtbls.infrastructure.persistence.db.sqlite.db_client_impl import (
    SQLiteDatabaseClientImpl,
)

logger = logging.getLogger(__name__)


async def init_db(sqlite_client: SQLiteDatabaseClientImpl, init_script_path: Path):
    async with sqlite_client.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        try:
            with init_script_path.open() as file:
                lines = file.readlines()
                for line in lines:
                    line = line.strip()
                    if line and not line.startswith("--"):
                        query = text(line.strip())
                        await conn.execute(
                            query, execution_options={"no_parameters": True}
                        )
        except Exception as ex:
            logger.exception(ex)
            raise ex


async def create_test_sqlite_db(
    file_path: Path, init_script_path: Path, scheme: str = "sqlite+aiosqlite"
):
    file_path.unlink(missing_ok=True)
    file_path.parent.mkdir(parents=True, exist_ok=True)

    sqlite_client = SQLiteDatabaseClientImpl(
        db_connection=SQLiteDatabaseConnection(
            url_scheme=scheme, file_path=str(file_path)
        )
    )

    await init_db(sqlite_client, init_script_path)


if __name__ == "__main__":
    file_path = Path("local_test.db")
    init_script_path = Path("local/sqlite/initial_data.sql")
    asyncio.run(create_test_sqlite_db(file_path, init_script_path))
