from models.scrape_task import TaskStatus, ScaraperTask, ScraperReq, WorkerScrapeTaskReq, WorkerScrapeTaskRes, FormatType
from services.worker_manager import get_worker_manager
from models.worker_status import WorkerStatus
from uuid import UUID, uuid4
from typing import List, Dict, Optional
from libs.worker_inetface import WorkerInterface
from utils.log import configure_logger
from threading import Thread, Lock
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from pydantic import BaseModel, Field
import time


logger = configure_logger(__file__)


# ============ Response Models ============

class ProcessDetailsResponse(BaseModel):
    process_id: str
    status: str
    progress_percentage: float
    total_urls: int
    completed_tasks: int
    successful_tasks: int
    failed_tasks: int
    urls: List[str]
    results: List[Dict] = Field(default_factory=list)
    
    class Config:
        json_encoders = {
            UUID: lambda v: str(v),
        }


class ResultPreview(BaseModel):
    format: str
    time_taken: float
    status: str
    output_preview: str


class WorkerHealthDetail(BaseModel):
    name: str
    id: str
    active_tasks: int
    queued_tasks: int
    success_rate: str


class ProcessStats(BaseModel):
    active: int
    completed: int
    total: int


class TaskStats(BaseModel):
    currently_processing: int
    total_completed: int


class WorkerStats(BaseModel):
    active_workers: int
    worker_details: List[WorkerHealthDetail]


class ThreadPoolStats(BaseModel):
    max_workers: int


class HealthCheckResponse(BaseModel):
    status: str
    timestamp: str
    processes: ProcessStats
    tasks: TaskStats
    workers: WorkerStats
    thread_pool: ThreadPoolStats
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat(),
        }


class BulkTaskResponse(BaseModel):
    process_id: str
    message: str
    total_urls: int
    
    class Config:
        json_encoders = {
            UUID: lambda v: str(v),
        }


class WorkerSelectionResponse(BaseModel):
    worker_id: Optional[str]
    worker_name: Optional[str]
    score: Optional[float]
    message: str
    
    class Config:
        json_encoders = {
            UUID: lambda v: str(v),
        }


# ============ Task Manager ============

class TaskManager:
    def __init__(self, max_workers: int = 10):
        self._worker_manager = get_worker_manager()
        
        # Store all processes: process_id -> ScaraperTask
        self._processes: Dict[UUID, ScaraperTask] = {}
        
        # Store task to process mapping: task_id -> process_id
        self._task_to_process: Dict[UUID, UUID] = {}
        
        # Store task statuses: task_id -> (status, worker_id)
        self._task_statuses: Dict[UUID, tuple] = {}
        
        # Store pending tasks waiting for completion: task_id -> (worker_interface, initial_response)
        self._pending_tasks: Dict[UUID, tuple] = {}
        
        # Thread safety
        self._lock = Lock()
        
        # Thread pool for parallel execution
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        
        logger.info(f"TaskManager initialized with max_workers={max_workers}")
    
    
    def select_most_reliable_worker(self) -> WorkerSelectionResponse:
        """
        Select the most reliable worker based on:
        1. Success rate (successful_tasks / total_tasks)
        2. Average processing time
        3. Current load (active + queued tasks)
        """
        active_workers: List[WorkerStatus] = self._worker_manager.get_activate_workers()
        
        if not active_workers:
            logger.warning("No active workers available")
            return WorkerSelectionResponse(
                worker_id=None,
                worker_name=None,
                score=None,
                message="No active workers available"
            )
        
        best_worker = None
        best_score = -1
        
        for worker in active_workers:
            total_tasks = worker.successful_tasks + worker.failed_tasks
            
            # Skip workers with no history
            if total_tasks == 0:
                success_rate = 0.5  # Neutral score for new workers
            else:
                success_rate = worker.successful_tasks / total_tasks
            
            # Calculate current load (lower is better)
            current_load = worker.active_tasks + worker.queued_tasks
            load_penalty = current_load * 0.1  # Each task reduces score by 0.1
            
            # Calculate time penalty (faster is better)
            time_penalty = worker.avg_time * 0.01 if worker.avg_time > 0 else 0
            
            # Calculate overall score (higher is better)
            score = success_rate - load_penalty - time_penalty
            
            logger.debug(
                f"Worker {worker.worker_name} score: {score:.3f} "
                f"(success_rate={success_rate:.2f}, load={current_load}, avg_time={worker.avg_time:.2f})"
            )
            
            if score > best_score:
                best_score = score
                best_worker = worker
        
        if best_worker:
            logger.info(f"Selected worker: {best_worker.worker_name} (score={best_score:.3f})")
            return WorkerSelectionResponse(
                worker_id=str(best_worker.worker_id),
                worker_name=best_worker.worker_name,
                score=round(best_score, 3),
                message="Worker selected successfully"
            )
        
        return WorkerSelectionResponse(
            worker_id=None,
            worker_name=None,
            score=None,
            message="No suitable worker found"
        )
    
    
    def add_bulk_task(self, scrape_req: ScraperReq) -> BulkTaskResponse:
        """
        Add bulk scraping tasks and return a process ID.
        Tasks are executed in parallel.
        """
        process_id = uuid4()
        
        # Initialize process
        scraper_task = ScaraperTask(
            urls=scrape_req.urls,
            complete_percentage=0.0,
            complete_status=TaskStatus.PROCESSING,
            results=[]
        )
        
        with self._lock:
            self._processes[process_id] = scraper_task
        
        logger.info(f"Created bulk task process {process_id} with {len(scrape_req.urls)} URLs")
        
        # Start processing in background thread
        thread = Thread(target=self._process_bulk_task, args=(process_id, scrape_req.urls))
        thread.daemon = True
        thread.start()
        
        return BulkTaskResponse(
            process_id=str(process_id),
            message="Bulk task created and processing started",
            total_urls=len(scrape_req.urls)
        )
    
    
    def _process_bulk_task(self, process_id: UUID, urls: List[str]):
        """Process all URLs in parallel"""
        futures = []
        task_ids = []
        
        # Submit all tasks to thread pool
        for url in urls:
            task_id = uuid4()
            task_ids.append(task_id)
            
            with self._lock:
                self._task_to_process[task_id] = process_id
                self._task_statuses[task_id] = (TaskStatus.PROCESSING, None)
            
            future = self._executor.submit(self._execute_single_task, task_id, url)
            futures.append((future, task_id))
        
        # Wait for all tasks to complete
        completed = 0
        for future, task_id in futures:
            try:
                result = future.result()
                completed += 1
                self._update_process_progress(process_id, completed, len(urls), result)
            except Exception as e:
                logger.error(f"Task {task_id} failed with error: {e}")
                completed += 1
                # Create failed result
                failed_result = WorkerScrapeTaskRes(
                    time_take=0.0,
                    format=FormatType.MARKDOWN,
                    html_content="",
                    output_content=f"Error: {str(e)}",
                    status=TaskStatus.COMPLETE
                )
                self._update_process_progress(process_id, completed, len(urls), failed_result)
        
        # Mark process as complete
        with self._lock:
            if process_id in self._processes:
                self._processes[process_id].complete_status = TaskStatus.COMPLETE
        
        logger.info(f"Bulk task process {process_id} completed")
    
    
    def _execute_single_task(self, task_id: UUID, url: str) -> WorkerScrapeTaskRes:
        """Execute a single scraping task"""
        worker_response = self.select_most_reliable_worker()
        
        if not worker_response.worker_id:
            raise Exception("No available workers")
        
        worker_id = UUID(worker_response.worker_id)
        
        # Get worker interface
        worker_status = self._worker_manager.get_worker_status(worker_id)
        worker_interface = WorkerInterface(base_url=worker_status.address)
        
        with self._lock:
            self._task_statuses[task_id] = (TaskStatus.PROCESSING, worker_id)
        
        logger.info(f"Executing task {task_id} on worker {worker_id} for URL: {url}")
        
        try:
            # Execute scraping - this initiates the task
            response = worker_interface.scrape(task_id=task_id, url=url, format="md")
            
            # Check if response is an acknowledgment or final result
            if "status" in response and response["status"] == "processing":
                # Task is processing, we need to wait for completion via callback
                # For now, we'll poll or wait for the task_complete callback
                logger.info(f"Task {task_id} is processing asynchronously on worker {worker_id}")
                
                # Store pending task info
                with self._lock:
                    self._pending_tasks[task_id] = (worker_interface, response)
                
                # Wait for task to complete (polling mechanism)
                # In a production system, you'd use webhooks or a message queue
                max_wait_time = 300  # 5 minutes
                poll_interval = 2  # 2 seconds
                elapsed = 0
                
                while elapsed < max_wait_time:
                    time.sleep(poll_interval)
                    elapsed += poll_interval
                    
                    # Check if task completed via callback
                    with self._lock:
                        if task_id not in self._pending_tasks:
                            # Task completed via callback
                            # Find the result in the process
                            process_id = self._task_to_process.get(task_id)
                            if process_id and process_id in self._processes:
                                process = self._processes[process_id]
                                # Find the result for this task
                                for result in process.results:
                                    # This is a simplified check - you might need a better way to match
                                    return result
                    
                    logger.debug(f"Waiting for task {task_id} to complete... ({elapsed}s elapsed)")
                
                # Timeout - create error result
                raise Exception(f"Task timeout after {max_wait_time} seconds")
            
            else:
                # Response is the final result
                result = WorkerScrapeTaskRes(**response)
                
                with self._lock:
                    self._task_statuses[task_id] = (TaskStatus.COMPLETE, worker_id)
                
                return result
                
        except Exception as e:
            logger.error(f"Error executing task {task_id}: {e}")
            raise
    
    
    def _update_process_progress(
        self, 
        process_id: UUID, 
        completed: int, 
        total: int, 
        result: WorkerScrapeTaskRes
    ):
        """Update process progress"""
        with self._lock:
            if process_id in self._processes:
                process = self._processes[process_id]
                process.results.append(result)
                process.complete_percentage = (completed / total) * 100
                logger.debug(
                    f"Process {process_id} progress: {process.complete_percentage:.1f}% "
                    f"({completed}/{total})"
                )
    
    
    def task_complete(
        self, 
        worker_id: UUID, 
        task_id: UUID, 
        worker_scrape_task_res: WorkerScrapeTaskRes
    ):
        """Handle task completion callback from worker"""
        with self._lock:
            # Remove from pending tasks
            if task_id in self._pending_tasks:
                del self._pending_tasks[task_id]
            
            if task_id in self._task_to_process:
                process_id = self._task_to_process[task_id]
                if process_id in self._processes:
                    process = self._processes[process_id]
                    process.results.append(worker_scrape_task_res)
                    
                    completed = len(process.results)
                    total = len(process.urls)
                    process.complete_percentage = (completed / total) * 100
                    
                    if completed >= total:
                        process.complete_status = TaskStatus.COMPLETE
                    
                    # Update task status
                    self._task_statuses[task_id] = (TaskStatus.COMPLETE, worker_id)
                    
                    logger.info(
                        f"Task {task_id} completed. Process {process_id} progress: "
                        f"{process.complete_percentage:.1f}%"
                    )
    
    
    def get_process_details(self, process_id: UUID) -> Optional[ProcessDetailsResponse]:
        """
        Get detailed information about a process including:
        - Progress percentage
        - Current status
        - Completed tasks
        - Failed tasks
        - Results (if complete)
        """
        with self._lock:
            if process_id not in self._processes:
                return None
            
            process = self._processes[process_id]
            
            successful_results = [
                r for r in process.results 
                if r.status == TaskStatus.COMPLETE and "Error:" not in r.output_content
            ]
            failed_results = [
                r for r in process.results 
                if "Error:" in r.output_content
            ]
            
            results_data = []
            if process.complete_status == TaskStatus.COMPLETE:
                results_data = [
                    {
                        "format": r.format.value,
                        "time_taken": r.time_take,
                        "status": r.status.value,
                        "output_preview": r.output_content
                        if len(r.output_content) > 200 else r.output_content
                    }
                    for r in process.results
                ]
            
            return ProcessDetailsResponse(
                process_id=str(process_id),
                status=process.complete_status.value,
                progress_percentage=process.complete_percentage,
                total_urls=len(process.urls),
                completed_tasks=len(process.results),
                successful_tasks=len(successful_results),
                failed_tasks=len(failed_results),
                urls=process.urls,
                results=results_data
            )
    
    
    def health_check(self) -> HealthCheckResponse:
        """
        Return health information about the task manager:
        - Total active processes
        - Total completed processes
        - Current processing tasks
        - Worker statistics
        """
        with self._lock:
            active_processes = [
                p for p in self._processes.values() 
                if p.complete_status == TaskStatus.PROCESSING
            ]
            completed_processes = [
                p for p in self._processes.values() 
                if p.complete_status == TaskStatus.COMPLETE
            ]
            
            total_tasks_processing = sum(
                len(p.urls) - len(p.results) for p in active_processes
            )
            
            active_workers = self._worker_manager.get_activate_workers()
            
            worker_details = [
                WorkerHealthDetail(
                    name=w.worker_name,
                    id=str(w.worker_id),
                    active_tasks=w.active_tasks,
                    queued_tasks=w.queued_tasks,
                    success_rate=f"{(w.successful_tasks / (w.successful_tasks + w.failed_tasks) * 100):.1f}%"
                    if (w.successful_tasks + w.failed_tasks) > 0 else "N/A"
                )
                for w in active_workers
            ]
            
            return HealthCheckResponse(
                status="healthy",
                timestamp=datetime.now().isoformat(),
                processes=ProcessStats(
                    active=len(active_processes),
                    completed=len(completed_processes),
                    total=len(self._processes)
                ),
                tasks=TaskStats(
                    currently_processing=total_tasks_processing,
                    total_completed=sum(len(p.results) for p in completed_processes)
                ),
                workers=WorkerStats(
                    active_workers=len(active_workers),
                    worker_details=worker_details
                ),
                thread_pool=ThreadPoolStats(
                    max_workers=self._executor._max_workers
                )
            )


# Singleton instance
_task_manager: TaskManager = None

def get_task_manager() -> TaskManager:
    global _task_manager
    if _task_manager is None:
        _task_manager = TaskManager()
    return _task_manager