"""
Docstring for worker_node.utils.config
"""

from pydantic import BaseModel
from uuid import UUID, uuid4
import json
import random
from enum import Enum

class Protocol(str, Enum):
    HTTP = "http"
    HTTPS = "https"

class Config(BaseModel):
    worker_id: UUID
    worker_name: str
    workers: int
    port: int
    host: str
    master_endpoint: str
    wait_time : int
    headless : bool
    protocol : Protocol
    use_callbacks : bool


_config: Config = None


# ---- random name generator ----
def generate_random_worker_name(prefix: str = "worker") -> str:
    adjectives = [
        "fast", "silent", "brave", "sharp", "iron",
        "dark", "swift", "frost", "wild", "atomic"
    ]
    nouns = [
        "wolf", "hawk", "tiger", "eagle", "panther",
        "cobra", "falcon", "lynx", "bear", "viper"
    ]

    return f"{prefix}-{random.choice(adjectives)}-{random.choice(nouns)}"


def get_config():
    global _config
    if _config is None:
        with open("config.json", "r") as file:
            raw_config = json.load(file)

            _config = Config(
                worker_id=uuid4(),
                worker_name=generate_random_worker_name(),
                **raw_config
            )
    return _config
