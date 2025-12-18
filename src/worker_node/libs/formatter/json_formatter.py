from libs.formatter.base_formatter import BaseFormatter
from models.content import ContentItem, ContentType
from models.scrape_task import FormatType

from typing import List, Dict
import json




class JsonFormatter(BaseFormatter):
    """
    Converts a Content object into structured JSON format.
    """


    def __init__(self, url: str):
        super().__init__(url, FormatType.JSON)


    def get_doc(self, content: List[ContentItem]) -> str:
        """
        Convert Content object into JSON string with categorized content.
        """
        # Build the structured output
        output = {
            "metadata": self._generate_metadata(content),
            "text": [],
            "img": [],
            "href": []
        }
       
        # Process content items
        if content:
            for item in content:
                if item.content_type == ContentType.TEXT:
                    text = self._clean_text(item.content)
                    if text:
                        output["text"].append(text)
               
                elif item.content_type == ContentType.IMG:
                    if self._is_valid_url(item.content):
                        output["img"].append(item.content)
               
                elif item.content_type == ContentType.URL:
                    if self._is_valid_url(item.content):
                        output["href"].append(item.content)
       
        # Convert to JSON string with pretty formatting
        return json.dumps(output, indent=2, ensure_ascii=False)


    def _generate_metadata(self, content: List[ContentItem]) -> Dict:
        """Generate metadata dictionary."""
        metadata = {
            "source_url": self._url
        }
       

       
        return metadata


    def _clean_text(self, text: str) -> str:
        """Clean and normalize text content."""
        if not text:
            return ""
       
        # Strip whitespace
        text = text.strip()
       
        # Remove multiple consecutive spaces
        while "  " in text:
            text = text.replace("  ", " ")
       
        return text


    def _is_valid_url(self, url: str) -> bool:
        """Check if URL is valid (not an internal anchor link)."""
        if not url or not isinstance(url, str):
            return False
       
        url = url.strip()
       
        # Filter out internal/anchor links
        if url.startswith("#"):
            return False
       
        # Filter out javascript: and mailto: links
        if url.startswith(("javascript:", "mailto:")):
            return False
       
        # Must have some meaningful content
        if len(url) < 3:
            return False
       
        return True



