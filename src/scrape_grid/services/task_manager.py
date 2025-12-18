from models.scrape_task import TaskStatus, ScaraperTask, ScraperReq
from services.worker_manager import get_worker_manager
from models.worker_status import WorkerStatus
from uuid import UUID, uuid4
from typing import List

class TaskManager:
    def __init__(self):
        self._worker_manager = get_worker_manager()
    
    def select_worker(self) -> UUID:
        "use workstaus list to slect the worker"
        active_worker_list : List[WorkerStatus] = self._worker_manager.get_activate_workers()