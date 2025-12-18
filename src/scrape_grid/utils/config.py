"""
Docstring for worker_node.utils.config
"""

from pydantic import BaseModel
from uuid import UUID, uuid4
import json
import random


class Config(BaseModel):
    port: int
    host: str


_config: Config = None



def get_config():
    global _config
    if _config is None:
        with open("config.json", "r") as file:
            raw_config = json.load(file)

            _config = Config(
                **raw_config
            )
    return _config
