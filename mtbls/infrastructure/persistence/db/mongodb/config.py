from typing import Optional

from pydantic import BaseModel


class MongoDbConnection(BaseModel):
    url_scheme: str = ""
    host: str = ""
    port: int = -1
    user: str = ""
    password: str = ""
    database: str = ""
    auth_source: Optional[str] = "admin"


class MongoDbConfiguration(BaseModel):
    connection: MongoDbConnection = MongoDbConnection()
