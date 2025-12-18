from uuid import UUID
from typing import Literal
import requests


class WorkerInterface:
    def __init__(self, base_url: str, timeout: int = 30):
        """
        :param base_url: e.g. http://127.0.0.1:9000
        :param timeout: request timeout in seconds
        """
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def scrape(
        self,
        task_id: UUID,
        url: str,
        format: Literal["md", "json"] = "md",
    ) -> dict:
        endpoint = f"{self.base_url}/scrape"

        payload = {
            "task_id": str(task_id),
            "url": url,
            "format": format,
        }

        response = requests.post(
            endpoint,
            json=payload,
            headers={
                "accept": "application/json",
                "Content-Type": "application/json",
            },
            timeout=self.timeout,
        )

        # raise error for 4xx / 5xx
        response.raise_for_status()

        return response.json()
