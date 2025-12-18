from models.status import Status
from utils.config import get_config
from utils.log import configure_logger
from time import time
from models.scrape_task import ScrapeTaskReq, ScrapeTask, ScrapeTaskStatus, ScrapeTaskRes
from concurrent.futures import ThreadPoolExecutor, Future
from queue import Queue
from uuid import UUID, uuid4
from libs.scrape_worker import ScrapeWorker, ScrapeWorkerError, BrowserLaunchError, NavigationError, HTMLExtractionError, ContentParsingError
from typing import Dict, Optional, List
from threading import Lock


logger = configure_logger(__file__)


class TaskManager:
    """
    Task manager for handling web scraping tasks with thread pool execution.
    """
    
    def __init__(self):
        """Initialize the TaskManager with metrics and thread pool."""
        self._successful_tasks = 0
        self._failed_tasks = 0
        self._active_tasks = 0
        self._queued_tasks = 0
        self._avg_time = 0.0
        self._total_time = 0.0
        self.__config = get_config()
        self._thread_pool = ThreadPoolExecutor(max_workers=self.__config.workers)
        self.__queue: Queue = Queue()
        
        # Store task results and futures
        self._tasks: Dict[UUID, Dict] = {}
        self._lock = Lock()
        
        logger.info(
            "TaskManager initialized",
            extra={
                "worker_name": self.__config.worker_name,
                "worker_id": str(self.__config.worker_id),
                "max_workers": self.__config.workers
            }
        )

    def add_task(self, scrape_task_req: ScrapeTaskReq) -> UUID:
        """
        Add a scraping task to the queue and submit it to the thread pool.
        
        Args:
            scrape_task_req: The scraping task request
            
        Returns:
            UUID: The task ID for tracking
        """
        task_id = scrape_task_req.task_id
        with self._lock:
            self._queued_tasks += 1
            
            # Initialize task tracking
            self._tasks[task_id] = {
                "status": ScrapeTaskStatus.PROCESSING,
                "request": scrape_task_req,
                "result": None,
                "error": None,
                "submit_time": time()
            }
        
        # Submit task to thread pool
        future: Future = self._thread_pool.submit(
            self._execute_task,
            task_id,
            scrape_task_req
        )
        
        # Add callback to handle completion
        future.add_done_callback(lambda f: self._task_done_callback(task_id, f))
        
        self._tasks[task_id]["future"] = future
        
        logger.info(
            "Task added to queue",
            extra={
                "task_id": str(task_id),
                "url": scrape_task_req.url,
                "format": scrape_task_req.format
            }
        )
        
        return task_id

    def _execute_task(self, task_id: UUID, scrape_task_req: ScrapeTaskReq) -> ScrapeTask:
        """
        Execute the scraping task using ScrapeWorker.
        
        Args:
            task_id: The task UUID
            scrape_task_req: The scraping request
            
        Returns:
            ScrapeTask: The completed scrape task
        """
        with self._lock:
            self._queued_tasks -= 1
            self._active_tasks += 1
        
        logger.info(
            "Task execution started",
            extra={
                "task_id": str(task_id),
                "url": scrape_task_req.url
            }
        )
        
        try:
            # Create and run scrape worker
            worker = ScrapeWorker(
                url=scrape_task_req.url,
                format=scrape_task_req.format,
                wait_time=10,
                headless=True
            )
            
            scrape_task = worker.run()
            
            logger.info(
                "Task execution completed successfully",
                extra={
                    "task_id": str(task_id),
                    "duration": scrape_task.end_exec_time - scrape_task.start_exec_time
                }
            )
            
            return scrape_task
            
        except (ScrapeWorkerError, BrowserLaunchError, NavigationError, 
                HTMLExtractionError, ContentParsingError) as e:
            logger.error(
                "Task execution failed",
                extra={
                    "task_id": str(task_id),
                    "error": str(e),
                    "error_type": type(e).__name__
                },
                exc_info=True
            )
            raise
        except Exception as e:
            logger.exception(
                "Unexpected error during task execution",
                extra={"task_id": str(task_id)}
            )
            raise

    def _task_done_callback(self, task_id: UUID, future: Future):
        """
        Callback executed when a task completes (success or failure).
        
        Args:
            task_id: The task UUID
            future: The Future object from the thread pool
        """
        with self._lock:
            self._active_tasks -= 1
            
            try:
                # Get the result from the future
                scrape_task: ScrapeTask = future.result()
                
                # Calculate execution time
                exec_time = scrape_task.end_exec_time - scrape_task.start_exec_time
                
                # Update running average
                total_completed = self._successful_tasks + 1
                self._total_time += exec_time
                self._avg_time = self._total_time / total_completed
                
                # Update task info
                self._tasks[task_id]["status"] = ScrapeTaskStatus.COMPLETE
                self._tasks[task_id]["result"] = scrape_task
                self._tasks[task_id]["error"] = None
                
                self._successful_tasks += 1
                
                logger.info(
                    "Task completed successfully",
                    extra={
                        "task_id": str(task_id),
                        "execution_time": exec_time,
                        "avg_time": self._avg_time
                    }
                )
                
            except Exception as e:
                # Task failed
                self._tasks[task_id]["status"] = ScrapeTaskStatus.COMPLETE
                self._tasks[task_id]["result"] = None
                self._tasks[task_id]["error"] = str(e)
                
                self._failed_tasks += 1
                
                logger.error(
                    "Task failed",
                    extra={
                        "task_id": str(task_id),
                        "error": str(e)
                    }
                )

    def get_task_status(self, task_id: UUID) -> Optional[ScrapeTaskRes]:
        """
        Get the status and result of a task.
        
        Args:
            task_id: The task UUID
            
        Returns:
            ScrapeTaskRes: The task response with status and results, or None if not found
        """
        with self._lock:
            task_info = self._tasks.get(task_id)
            
            if not task_info:
                logger.warning(
                    "Task not found",
                    extra={"task_id": str(task_id)}
                )
                return None
            
            status = task_info["status"]
            result = task_info["result"]
            error = task_info["error"]
            request = task_info["request"]
            
            # If task is still processing
            if status == ScrapeTaskStatus.PROCESSING:
                return ScrapeTaskRes(
                    time_take=0.0,
                    format=request.format,
                    html_content="",
                    output_content="",
                    status=ScrapeTaskStatus.PROCESSING
                )
            
            # If task failed
            if error:
                return ScrapeTaskRes(
                    time_take=0.0,
                    format=request.format,
                    html_content="",
                    output_content=f"Error: {error}",
                    status=ScrapeTaskStatus.COMPLETE
                )
            
            # Task completed successfully
            if result:
                return ScrapeTaskRes(
                    time_take=result.end_exec_time - result.start_exec_time,
                    format=result.format,
                    html_content=result.html_content,
                    output_content=result.output_content,
                    status=ScrapeTaskStatus.COMPLETE
                )
            
            return None

    def get_health(self) -> Status:
        """
        Get the current health status of the task manager.
        
        Returns:
            Status: Current worker status metrics including queued, active,
                   successful, and failed task counts, plus average execution time
        """
        with self._lock:
            health_status = Status(
                worker_name=self.__config.worker_name,
                worker_id=self.__config.worker_id,
                queued_tasks=self._queued_tasks,
                active_tasks=self._active_tasks,
                successful_tasks=self._successful_tasks,
                failed_tasks=self._failed_tasks,
                avg_time=self._avg_time,
                tick_time=time(),
                address=f"{self.__config.protocol.value}://{self.__config.host}:{self.__config.port}"
            )
            
            logger.debug(
                "Health status retrieved",
                extra={
                    "queued": self._queued_tasks,
                    "active": self._active_tasks,
                    "successful": self._successful_tasks,
                    "failed": self._failed_tasks,
                    "avg_time": self._avg_time
                }
            )
            
            return health_status

    def shutdown(self, wait: bool = True):
        """
        Shutdown the task manager and cleanup resources.
        
        Args:
            wait: Whether to wait for all tasks to complete
        """
        logger.info("Shutting down TaskManager")
        self._thread_pool.shutdown(wait=wait)
        logger.info("TaskManager shutdown complete")