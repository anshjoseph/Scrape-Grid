from abc import ABC, abstractmethod
from pathlib import Path
from models.content import ContentItem
from typing import List
import re
from utils.log import configure_logger


logger = configure_logger(__file__)




class BaseFormatter(ABC):
    """
    Abstract base class for content formatters.
    Subclasses must implement `get_doc` to generate content.
    Provides method to save generated docs according to SaveMethod.
    """


    def __init__(self, url:str, format: str):
        self._format = format
        self._url = url
        logger.debug(f"Initialized BaseFormat with format={format}")


    @property
    def format(self) -> str:
        return self._format


    @abstractmethod
    def get_doc(self, content: List[ContentItem]) -> str:
        """
        Generate formatted string from Content object.
        Must be implemented by subclass.
        """
        pass