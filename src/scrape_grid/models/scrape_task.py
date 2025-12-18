from pydantic import BaseModel
from uuid import UUID
from enum import Enum
from typing import List

class FormatType(str, Enum):
    JSON = "json"
    MARKDOWN = "md"


class TaskStatus(str, Enum):
    COMPLETE = "complete"
    PROCESSING = "processing"

class WorkerScrapeTaskReq(BaseModel):
    task_id : UUID
    url : str
    format : FormatType

class WorkerScrapeTaskRes(BaseModel):
    time_take : float
    format : FormatType
    html_content : str
    output_content : str
    status : TaskStatus


class ScraperReq(BaseModel):
    urls : List[str]

class ScaraperTask(BaseModel):
    urls : List[str]
    complete_percentage : float
    complete_status : TaskStatus
    results : List[WorkerScrapeTaskRes]
