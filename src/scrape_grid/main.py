from fastapi import FastAPI
from fastapi.responses import HTMLResponse
import uvicorn
from utils.config import get_config
from utils.log import configure_logger
from contextlib import asynccontextmanager

# routers
from routes.workers import get_workers_router
from routes.tasks import get_tasks_router

# services
from services.worker_manager import get_worker_manager
from services.task_manager import get_task_manager


logger = configure_logger(__file__)




@asynccontextmanager
async def lifespan(app: FastAPI):
    # init services
    logger.info("Initializing services...")
    get_worker_manager()
    get_task_manager()
    logger.info("Services initialized successfully")
    yield
    logger.info("Shutting down services...")



app = FastAPI(lifespan=lifespan, version="0.1.0", title="Scrape Grid")


# include routes
app.include_router(get_workers_router())
app.include_router(get_tasks_router())


@app.get("/info")
def root():
    return {
        "message": "Scrape Grid API",
        "version": "0.1.0",
        "status": "running"
    }
@app.get("/")
async def get_index_page():
    with open("index.html", 'r') as file:
        return HTMLResponse(content=file.read())


if __name__ == "__main__":
    config = get_config()
    logger.info(f"Starting server on {config.host}:{config.port}")
    uvicorn.run(
        app,
        host=config.host,
        port=config.port
    )