from fastapi import APIRouter, HTTPException, Path
from typing import Optional
from uuid import UUID

from services.task_manager import get_task_manager, BulkTaskResponse, ProcessDetailsResponse, HealthCheckResponse, WorkerSelectionResponse
from models.scrape_task import ScraperReq
from utils.log import configure_logger


logger = configure_logger(__file__)


def get_tasks_router() -> APIRouter:
    router = APIRouter(prefix="/tasks", tags=["tasks"])
    task_manager = get_task_manager()
    
    
    @router.post("/bulk", response_model=BulkTaskResponse)
    async def create_bulk_task(scrape_req: ScraperReq):
        """
        Create a bulk scraping task with multiple URLs.
        Returns a process ID to track the task progress.
        """
        try:
            logger.info(f"Creating bulk task with {len(scrape_req.urls)} URLs")
            response = task_manager.add_bulk_task(scrape_req)
            logger.info(f"Bulk task created: {response.process_id}")
            return response
        except Exception as e:
            logger.error(f"Failed to create bulk task: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to create bulk task: {str(e)}")
    
    
    @router.get("/process/{process_id}", response_model=ProcessDetailsResponse)
    async def get_process_status(
        process_id: UUID = Path(..., description="The process ID returned from bulk task creation")
    ):
        """
        Get detailed information about a specific process:
        - Progress percentage
        - Current status (processing/complete)
        - Number of completed, successful, and failed tasks
        - Results (if complete)
        """
        try:
            logger.info(f"Fetching process details for: {process_id}")
            details = task_manager.get_process_details(process_id)
            
            if details is None:
                logger.warning(f"Process not found: {process_id}")
                raise HTTPException(status_code=404, detail=f"Process {process_id} not found")
            
            return details
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Failed to fetch process details: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to fetch process details: {str(e)}")
    
    
    @router.get("/health", response_model=HealthCheckResponse)
    async def health_check():
        """
        Get health information about the task manager:
        - Number of active and completed processes
        - Current processing tasks
        - Worker statistics
        - Thread pool information
        """
        try:
            logger.info("Performing health check")
            health = task_manager.health_check()
            return health
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            raise HTTPException(status_code=500, detail=f"Health check failed: {str(e)}")
    
    
    @router.get("/worker/best", response_model=WorkerSelectionResponse)
    async def get_best_worker():
        """
        Get the most reliable worker based on:
        - Success rate
        - Average processing time
        - Current load
        """
        try:
            logger.info("Selecting best worker")
            response = task_manager.select_most_reliable_worker()
            return response
        except Exception as e:
            logger.error(f"Failed to select best worker: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to select best worker: {str(e)}")
    
    
    return router