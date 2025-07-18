from typing import Literal

from pydantic import BaseModel


class MtblsWs2AuthenticationConfiguration(BaseModel):
    host: str
    port: int
    base_context_path: str = "/metabolights/ws"
    scheme: str = Literal["http", "https"]

    def get_url(self):
        return f"{self.scheme}://{self.host}:{self.port}{self.base_context_path}"
