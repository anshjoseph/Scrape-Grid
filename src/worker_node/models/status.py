from pydantic import BaseModel
from uuid import UUID

class Status(BaseModel):
    worker_name: str
    worker_id: UUID
    queued_tasks: int
    active_tasks: int
    successful_tasks: int
    failed_tasks: int
    avg_time : float
    tick_time : float
    address : str
