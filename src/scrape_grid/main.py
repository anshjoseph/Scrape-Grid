from fastapi import FastAPI
import uvicorn
from utils.config import get_config
from utils.log import configure_logger
from contextlib import asynccontextmanager

# routers
from routes.workers import get_workers_router

# services
from services.worker_manager import get_worker_manager


logger = configure_logger(__file__)




@asynccontextmanager
async def lifespan(app: FastAPI):
    # init services
    get_worker_manager()
    yield



app = FastAPI(lifespan=lifespan, version="0.1.0", title="Scrape Grid")


# include routes
app.include_router(get_workers_router())



if __name__ == "__main__":
    config = get_config()
    uvicorn.run(
        app,
        host=config.host,
        port=config.port
    )