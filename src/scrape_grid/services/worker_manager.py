from typing import Dict, List
from models.worker_status import WorkerStatus
from uuid import UUID
from datetime import datetime
from utils.log import configure_logger


logger = configure_logger(__file__)


class WorkerManager:
    def __init__(self):
        self.__workers_status: Dict[UUID, WorkerStatus] = {}
    
    def add_or_update_worker_status(self, worker_name: str,
        worker_id: UUID,
        queued_tasks: int,
        active_tasks: int,
        successful_tasks: int,
        failed_tasks: int,
        avg_time : float,
        tick_time : float,
        address : str):
        work_status = self.__workers_status.get(worker_id)
        if work_status == None:
            logger.info(f"new worker is added, worker id : {worker_id} worker name : {worker_name}")
            self.__workers_status[worker_id] = WorkerStatus(
                worker_name=worker_name,
                worker_id=worker_id,
                queued_tasks=queued_tasks,
                active_tasks=active_tasks,
                avg_time=avg_time,
                tick_time=tick_time,
                failed_tasks=failed_tasks,
                successful_tasks=successful_tasks,
                register_timestamp=datetime.now(),
                last_sync_timestamp=datetime.now(),
                address=address
            )
        else:
            logger.info(f"upadting worker id : {worker_id} worker name : {worker_name} status")
            logger.info(f"FROM:\n{work_status}")
            self.__workers_status[worker_id] = WorkerStatus(
                worker_name=worker_name,
                worker_id=worker_id,
                queued_tasks=queued_tasks,
                active_tasks=active_tasks,
                avg_time=avg_time,
                tick_time=tick_time,
                successful_tasks=successful_tasks,
                failed_tasks=failed_tasks,
                register_timestamp=work_status.register_timestamp,
                last_sync_timestamp=datetime.now(),
                address=address
            )
            logger.info(f"TO:\n{self.__workers_status[worker_id]}")
    
    def remove_worker(self, worker_id:UUID) -> bool:
        work_status = self.__workers_status.get(worker_id)
        if work_status == None:
            logger.warning(f"there is no worker with id : {worker_id}")
            return False
        else:
            work_status = self.__workers_status.pop(worker_id)
            logger.info(f"worker with id : {work_status.worker_id} and name : {work_status.worker_name} is removed from system")
            logger.info(f"{work_status}")
            logger.info("-"*80)
            return True
    
    def get_activate_workers(self) -> List[WorkerStatus]:
        return list(self.__workers_status.values())
    
    def get_worker_status(self, worker_id:UUID):
        return self.__workers_status.get(worker_id)


_worker_manager : WorkerManager = None


def get_worker_manager():
    global _worker_manager
    if _worker_manager == None:
        _worker_manager = WorkerManager()
    return _worker_manager