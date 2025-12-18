from pydantic import BaseModel
from uuid import UUID
from datetime import datetime


class WorkerStatus(BaseModel):
    worker_name: str
    worker_id: UUID
    queued_tasks: int
    active_tasks: int
    successful_tasks: int
    failed_tasks: int
    avg_time : float
    tick_time : float
    address : str
    register_timestamp : datetime
    last_sync_timestamp : datetime
    class Config:
        json_encoders = {
            UUID: lambda v: str(v),
            datetime: lambda v: v.isoformat(),
        }