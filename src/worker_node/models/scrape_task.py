from pydantic import BaseModel
from uuid import UUID
from models.content import ContentItem
from enum import Enum
from typing import List

class FormatType(str, Enum):
    JSON = "json"
    MARKDOWN = "md"



class ScrapeTaskStatus(str, Enum):
    COMPLETE = "complete"
    PROCESSING = "processing"

class ScrapeTask(BaseModel):
    task_id : UUID
    url : str
    html_content_item : List[ContentItem]
    html_content : str
    output_content : str
    format : FormatType
    start_exec_time : float
    end_exec_time : float


class ScrapeTaskReq(BaseModel):
    task_id : UUID
    url : str
    format : FormatType

class ScrapeTaskRes(BaseModel):
    time_take : float
    format : FormatType
    html_content : str
    output_content : str
    status : ScrapeTaskStatus