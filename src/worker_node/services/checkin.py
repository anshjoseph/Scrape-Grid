from threading import Thread, Event
from utils.log import configure_logger
from utils.config import get_config
from services.task_manager import TaskManager
import time
from libs.master_interface import MasterInterface

logger = configure_logger(__file__)


class CheckIn(Thread):
    def __init__(self, task_manager: TaskManager):
        super().__init__(name="check_in_service", daemon=True)

        self._config = get_config()
        self._stop_event = Event()
        self._master = MasterInterface()
        self._task_manager = task_manager

        health = self._task_manager.get_health()
        self._worker_id = health.worker_id
        self._worker_name = health.worker_name

        self._interval = getattr(self._config, "checkin_interval", 5)

        logger.info(
            "CheckIn service initialized",
            extra={
                "worker_id": str(self._worker_id),
                "worker_name": self._worker_name,
                "interval": self._interval,
            },
        )

    # -------------------------
    # Graceful stop
    # -------------------------
    def stop(self):
        logger.info(
            "Stopping CheckIn service",
            extra={"worker_id": str(self._worker_id)},
        )
        self._stop_event.set()

    # -------------------------
    # Main loop
    # -------------------------
    def run(self):
        logger.info(
            "CheckIn service started",
            extra={"worker_id": str(self._worker_id)},
        )

        try:
            while not self._stop_event.is_set():
                health = self._task_manager.get_health()

                payload = {
                    "worker_name": health.worker_name,
                    "worker_id": str(health.worker_id),
                    "queued_tasks": int(health.queued_tasks),
                    "active_tasks": int(health.active_tasks),
                    "successful_tasks": int(health.successful_tasks),
                    "failed_tasks": int(health.failed_tasks),
                    "avg_time": float(health.avg_time),
                    "tick_time": float(time.time()),   # ✅ REQUIRED FIX
                    "address": str(health.address),    # ✅ REQUIRED FIELD
                }

                logger.debug(
                    "Sending worker check-in",
                    extra={"payload": payload},
                )

                success = self._master.checkin_worker(payload)

                if not success:
                    logger.warning(
                        "Worker check-in failed",
                        extra={"worker_id": str(self._worker_id)},
                    )

                time.sleep(self._interval)

        except Exception as e:
            logger.error(
                "CheckIn service crashed",
                extra={
                    "worker_id": str(self._worker_id),
                    "error": str(e),
                },
                exc_info=True,
            )

        finally:
            logger.info(
                "CheckIn service shutting down, checking out worker",
                extra={"worker_id": str(self._worker_id)},
            )

            try:
                self._master.checkout_worker(self._worker_id)
            except Exception as e:
                logger.error(
                    "Worker checkout failed during shutdown",
                    extra={
                        "worker_id": str(self._worker_id),
                        "error": str(e),
                    },
                    exc_info=True,
                )
