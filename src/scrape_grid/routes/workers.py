from fastapi import APIRouter, status, Depends, Response, HTTPException
from pydantic import BaseModel
from uuid import UUID
from services.worker_manager import get_worker_manager, WorkerManager
from models.scrape_task import WorkerScrapeTaskRes

# appi router
_router = APIRouter(prefix="/workers", tags=["Workers"])
def get_workers_router():
    return _router


# dto's
class CheckInReq(BaseModel):
    worker_name: str
    worker_id: UUID
    queued_tasks: int
    active_tasks: int
    successful_tasks: int
    failed_tasks: int
    avg_time : float
    tick_time : float
    address : str



# routes
@_router.post("/checkin")
async def check_in_workers(check_in_req: CheckInReq, worker_manager:WorkerManager = Depends(get_worker_manager)):
    worker_manager.add_or_update_worker_status(
        worker_id=check_in_req.worker_id,
        worker_name=check_in_req.worker_name,
        queued_tasks=check_in_req.queued_tasks,
        active_tasks=check_in_req.active_tasks,
        successful_tasks=check_in_req.successful_tasks,
        failed_tasks=check_in_req.failed_tasks,
        avg_time=check_in_req.avg_time,
        address=check_in_req.address,
        tick_time=check_in_req.tick_time
    )
    return Response(
        content="worker is added succesfully",
        status_code=status.HTTP_201_CREATED
    )

@_router.delete("/checkout")
async def check_out_workers(worker_id: UUID, worker_manager:WorkerManager = Depends(get_worker_manager)):
    if worker_manager.remove_worker(worker_id):
        return Response(
        status_code=status.HTTP_204_NO_CONTENT
    )
    return Response(
        content="worker is added succesfully",
        status_code=status.HTTP_404_NOT_FOUND
    )


@_router.get("/")
async def get_workers(worker_manager:WorkerManager = Depends(get_worker_manager)):
    return worker_manager.get_activate_workers()
@_router.get("/{worker_id}")
async def get_workers(worker_id:UUID,  worker_manager:WorkerManager = Depends(get_worker_manager)):
    worker_status = worker_manager.get_worker_status(worker_id)
    if worker_status:
        return worker_status
    return HTTPException(status_code=status.HTTP_404_NOT_FOUND)

@_router.post("/{worker_id}/jobs/{task_id}/add-result")
async def add_job_after_complete(worker_id:UUID, task_id:UUID, data: WorkerScrapeTaskRes):
    pass