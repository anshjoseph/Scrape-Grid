from utils.config import get_config
import requests
from uuid import UUID
from typing import Dict, Any
from utils.log import configure_logger

logger = configure_logger(__file__)


class MasterInterface:
    def __init__(self):
        self._config = get_config()
        self._base_url: str = self._config.master_endpoint.rstrip("/")
        self._timeout = 10

        logger.info(
            "MasterInterface initialized",
            extra={"base_url": self._base_url},
        )

    # -------------------------------------------------
    # Worker check-in
    # -------------------------------------------------
    def checkin_worker(self, payload: Dict[str, Any]) -> bool:
        url = f"{self._base_url}/checkin"

        logger.info(
            "Worker check-in started",
            extra={"url": url},
        )

        try:
            resp = requests.post(
                url,
                json=payload,
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                },
                timeout=self._timeout,
            )

            if resp.status_code == 201:
                logger.info(
                    "Worker check-in successful",
                    extra={"status_code": resp.status_code},
                )
                return True

            # 🔴 IMPORTANT: log FastAPI validation error
            logger.error(
                "Worker check-in rejected by master",
                extra={
                    "status_code": resp.status_code,
                    "response_body": resp.text,
                    "payload": payload,
                },
            )
            return False

        except requests.RequestException as e:
            logger.error(
                "Worker check-in request failed",
                extra={"error": str(e)},
                exc_info=True,
            )
            return False

    # -------------------------------------------------
    # Worker checkout
    # -------------------------------------------------
    def checkout_worker(self, worker_id: UUID) -> bool:
        url = f"{self._base_url}/checkout"

        logger.info(
            "Worker checkout started",
            extra={"worker_id": str(worker_id)},
        )

        try:
            resp = requests.delete(
                url,
                params={"worker_id": str(worker_id)},
                headers={"Accept": "application/json"},
                timeout=self._timeout,
            )

            if resp.status_code == 204:
                logger.info(
                    "Worker checkout successful",
                    extra={"worker_id": str(worker_id)},
                )
                return True

            logger.error(
                "Worker checkout rejected by master",
                extra={
                    "status_code": resp.status_code,
                    "response_body": resp.text,
                    "worker_id": str(worker_id),
                },
            )
            return False

        except requests.RequestException as e:
            logger.error(
                "Worker checkout request failed",
                extra={"worker_id": str(worker_id), "error": str(e)},
                exc_info=True,
            )
            return False

    # -------------------------------------------------
    # Job result submission
    # -------------------------------------------------
    def add_job_result(self, worker_id: UUID, payload: Dict[str, Any]) -> bool:
        url = f"{self._base_url}/jobs/{worker_id}/add-result"

        logger.info(
            "Submitting job result",
            extra={"worker_id": str(worker_id)},
        )

        try:
            resp = requests.post(
                url,
                json=payload,
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                },
                timeout=self._timeout,
            )

            if resp.status_code == 200:
                logger.info(
                    "Job result submitted successfully",
                    extra={"worker_id": str(worker_id)},
                )
                return True

            logger.error(
                "Job result submission rejected by master",
                extra={
                    "status_code": resp.status_code,
                    "response_body": resp.text,
                    "worker_id": str(worker_id),
                    "payload": payload,
                },
            )
            return False

        except requests.RequestException as e:
            logger.error(
                "Job result submission request failed",
                extra={"worker_id": str(worker_id), "error": str(e)},
                exc_info=True,
            )
            return False
