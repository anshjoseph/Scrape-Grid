from enum import Enum
from typing import List, Optional
from datetime import datetime

from pydantic import BaseModel, Field


class ContentType(str, Enum):
    TEXT = "TEXT"
    IMG = "IMG"
    URL = "URL"


class ContentItem(BaseModel):
    idx: int
    content_type: ContentType
    content: str


