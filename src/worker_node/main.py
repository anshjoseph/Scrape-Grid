from fastapi import FastAPI, HTTPException, status
from fastapi.responses import JSONResponse
import uvicorn
from contextlib import asynccontextmanager
from utils.config import get_config
from utils.log import configure_logger
from services.task_manager import TaskManager
from services.checkin import CheckIn
from models.scrape_task import ScrapeTaskReq, ScrapeTaskRes
from models.status import Status
from uuid import UUID
from typing import Dict


logger = configure_logger(__file__)

# Global task manager instance
task_manager: TaskManager = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager for startup and shutdown events.
    """
    global task_manager
    
    # Startup
    config = get_config()
    logger.info(f"Config: {config}")
    logger.info(f"Worker {config.worker_name} is starting...")
    
    # Initialize TaskManager
    task_manager = TaskManager()
    logger.info(f"TaskManager initialized with {config.workers} workers")
    logger.info(f"Worker {config.worker_name} is started")

    # init check in service
    check_in_service : CheckIn = CheckIn(task_manager)
    check_in_service.start()
    yield
    
    # Shutdown
    logger.info(f"Worker {config.worker_name} is shutting down...")
    if task_manager:
        task_manager.shutdown(wait=True)
    logger.info(f"Worker {config.worker_name} is closed")

    check_in_service.stop()


app = FastAPI(
    title="Web Scraper API",
    description="A distributed web scraping service using Playwright",
    version="1.0.0",
    lifespan=lifespan
)


# =========================
# API ENDPOINTS
# =========================

@app.get(
    "/",
    tags=["Health"],
    summary="Root endpoint",
    description="Returns a welcome message"
)
async def root() -> Dict[str, str]:
    """
    Root endpoint returning API information.
    """
    return {
        "message": "Web Scraper API",
        "version": "1.0.0",
        "status": "running"
    }


@app.get(
    "/health",
    tags=["Health"],
    summary="Health check",
    description="Get the current health status of the worker",
    response_model=Status
)
async def health() -> Status:
    """
    Get current worker health metrics including:
    - Worker name and ID
    - Active, queued, successful, and failed task counts
    - Average execution time
    """
    if not task_manager:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="TaskManager not initialized"
        )
    
    health_status = task_manager.get_health()
    logger.debug(f"Health check: {health_status}")
    return health_status


@app.post(
    "/scrape",
    tags=["Scraping"],
    summary="Submit scraping task",
    description="Submit a new web scraping task and receive a task ID for tracking",
    response_model=Dict[str, str],
    status_code=status.HTTP_202_ACCEPTED
)
async def scrape(scrape_req: ScrapeTaskReq) -> Dict[str, str]:
    """
    Submit a new scraping task.
    
    Args:
        scrape_req: Scraping task request containing URL and format
        
    Returns:
        Dictionary with task_id and message
    """
    if not task_manager:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="TaskManager not initialized"
        )
    
    try:
        task_id = task_manager.add_task(scrape_req)
        
        logger.info(
            f"Scraping task submitted",
            extra={
                "task_id": str(task_id),
                "url": scrape_req.url,
                "format": scrape_req.format
            }
        )
        
        return {
            "task_id": str(task_id),
            "message": "Task submitted successfully",
            "status": "processing"
        }
        
    except Exception as e:
        logger.error(f"Failed to submit task: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to submit task: {str(e)}"
        )


@app.get(
    "/task/{task_id}",
    tags=["Scraping"],
    summary="Get task status",
    description="Retrieve the status and results of a scraping task by ID",
    response_model=ScrapeTaskRes
)
async def get_task(task_id: str) -> ScrapeTaskRes:
    """
    Get the status and result of a scraping task.
    
    Args:
        task_id: The UUID of the task to retrieve
        
    Returns:
        ScrapeTaskRes: Task response with status and results
    """
    if not task_manager:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="TaskManager not initialized"
        )
    
    try:
        # Convert string to UUID
        task_uuid = UUID(task_id)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid task ID format"
        )
    
    task_result = task_manager.get_task_status(task_uuid)
    
    if not task_result:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Task with ID {task_id} not found"
        )
    
    logger.debug(
        f"Task status retrieved",
        extra={
            "task_id": task_id,
            "status": task_result.status
        }
    )
    
    return task_result


@app.delete(
    "/task/{task_id}",
    tags=["Scraping"],
    summary="Delete task",
    description="Delete a completed task from memory (not implemented)",
    status_code=status.HTTP_501_NOT_IMPLEMENTED
)
async def delete_task(task_id: str) -> Dict[str, str]:
    """
    Delete a task (placeholder for future implementation).
    """
    return {
        "message": "Task deletion not yet implemented",
        "task_id": task_id
    }


@app.get(
    "/tasks",
    tags=["Scraping"],
    summary="List all tasks",
    description="Get a summary of all tasks (not implemented)",
    status_code=status.HTTP_501_NOT_IMPLEMENTED
)
async def list_tasks() -> Dict[str, str]:
    """
    List all tasks (placeholder for future implementation).
    """
    return {
        "message": "Task listing not yet implemented"
    }


# =========================
# EXCEPTION HANDLERS
# =========================

@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    """
    Custom HTTP exception handler for better error responses.
    """
    logger.warning(
        f"HTTP Exception: {exc.status_code}",
        extra={
            "path": request.url.path,
            "detail": exc.detail
        }
    )
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.detail,
            "status_code": exc.status_code
        }
    )


@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    """
    General exception handler for unexpected errors.
    """
    logger.exception(
        "Unhandled exception",
        extra={"path": request.url.path}
    )
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error": "Internal server error",
            "detail": str(exc)
        }
    )


# =========================
# MAIN
# =========================

if __name__ == "__main__":
    config = get_config()
    logger.info(f"Starting server on {config.host}:{config.port}")
    
    uvicorn.run(
        app,
        host=config.host,
        port=config.port,
        log_config=None  # Use our custom logger
    )