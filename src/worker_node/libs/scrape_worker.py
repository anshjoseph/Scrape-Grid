import asyncio
import time
from uuid import uuid4

from playwright.async_api import async_playwright
from playwright_stealth import Stealth
from utils.log import configure_logger
from bs4 import BeautifulSoup, NavigableString, Tag
from models.scrape_task import ScrapeTask, FormatType
from models.content import ContentItem, ContentType
from typing import List
from libs.formatter import format_html


logger = configure_logger(__file__)


# =========================
# EXCEPTIONS
# =========================

class ScrapeWorkerError(Exception):
    """Base exception for ScrapeWorker."""


class BrowserLaunchError(ScrapeWorkerError):
    """Raised when browser fails to launch."""


class NavigationError(ScrapeWorkerError):
    """Raised when page navigation fails."""


class HTMLExtractionError(ScrapeWorkerError):
    """Raised when HTML extraction fails."""

class ContentParsingError(Exception):
    """html parsing error"""


# =========================
# SCRAPE WORKER
# =========================

class ScrapeWorker:
    """
    Scrape worker with Playwright + stealth.
    Responsible only for fetching rendered HTML.
    """

    def __init__(
        self,
        url: str,
        format: FormatType,
        wait_time: int = 10,
        headless: bool = True,
    ):
        self.url = url
        self.format = format
        self.wait_time = wait_time
        self.headless = headless

        logger.debug(
            "ScrapeWorker initialized",
            extra={
                "url": self.url,
                "format": self.format,
                "wait_time": self.wait_time,
                "headless": self.headless,
            },
        )

    # -------------------------
    # PUBLIC SYNC API
    # -------------------------
    def run(self) -> ScrapeTask:
        """
        Synchronous entrypoint for scraping.
        """
        return asyncio.run(self._run_async())
    
    @staticmethod
    def _extract_content_items(html: str) -> List[ContentItem]:
        """
        Extract content items from HTML.
        
        Args:
            html: The HTML string to parse
            
        Returns:
            List[ContentItem]: List of extracted content items
        """
        try:
            logger.debug("Starting HTML parsing.")
            soup = BeautifulSoup(html, "html.parser")
            items: list[ContentItem] = []
            idx = 1

            if not soup.body:
                logger.warning("No <body> found in HTML.")
                return items

            for el in soup.body.descendants:

                if isinstance(el, NavigableString):
                    text = el.strip()
                    if text:
                        items.append(ContentItem(idx=idx, content_type=ContentType.TEXT, content=text))
                        idx += 1

                elif isinstance(el, Tag):

                    if el.name == "img" and el.get("src"):
                        items.append(ContentItem(idx=idx, content_type=ContentType.IMG, content=el["src"]))
                        idx += 1

                    elif el.name == "a" and el.get("href"):
                        items.append(ContentItem(idx=idx, content_type=ContentType.URL, content=el["href"]))
                        idx += 1

            logger.info(f"Extracted {len(items)} content items from HTML.")
            return items

        except Exception as e:
            logger.exception("Failed to parse HTML content.")
            raise ContentParsingError("Failed to parse HTML content")

    async def _run_async(self) -> ScrapeTask:
        """
        Async method to perform the actual scraping.
        
        Returns:
            ScrapeTask: The completed scrape task with results
        """
        task_id = uuid4()
        start_exec_time = time.time()

        logger.info(
            "Scrape task started",
            extra={"task_id": str(task_id), "url": self.url},
        )

        html: str = ""

        try:
            async with Stealth().use_async(async_playwright()) as p:
                try:
                    logger.debug("Launching Chromium browser")
                    browser = await p.chromium.launch(
                        headless=self.headless,
                        args=[
                            "--disable-blink-features=AutomationControlled",
                            "--no-sandbox",
                            "--disable-dev-shm-usage",
                        ],
                    )
                except Exception as e:
                    logger.exception("Browser launch failed")
                    raise BrowserLaunchError(str(e)) from e

                context = await browser.new_context(
                    viewport={"width": 1366, "height": 768},
                    locale="en-US",
                    user_agent=(
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36"
                    ),
                )

                page = await context.new_page()

                try:
                    logger.info("Navigating to URL", extra={"url": self.url})
                    await page.goto(self.url, wait_until="load")
                    await page.wait_for_load_state("networkidle")
                    await asyncio.sleep(self.wait_time)
                except Exception as e:
                    logger.exception("Navigation failed")
                    await browser.close()
                    raise NavigationError(str(e)) from e

                try:
                    logger.debug("Extracting HTML content")
                    html = await page.content()
                    logger.info(
                        "HTML extracted",
                        extra={"html_size": len(html)},
                    )
                except Exception as e:
                    logger.exception("HTML extraction failed")
                    raise HTMLExtractionError(str(e)) from e
                finally:
                    await browser.close()

        except ScrapeWorkerError:
            logger.error(
                "Scrape task failed",
                extra={"task_id": str(task_id)},
                exc_info=True,
            )
            raise

        end_exec_time = time.time()
        
        # Extract content items from HTML
        html_content_item = self._extract_content_items(html)
        
        # Create the scrape task result
        scrape_task = ScrapeTask(
            task_id=task_id,
            url=self.url,
            html_content_item=html_content_item,
            html_content=html,
            output_content=format_html(self.url, html_content_item, self.format),
            format=self.format,
            start_exec_time=start_exec_time,
            end_exec_time=end_exec_time,
        )

        logger.info(
            "Scrape task completed",
            extra={
                "task_id": str(task_id),
                "duration_sec": end_exec_time - start_exec_time,
            },
        )

        return scrape_task