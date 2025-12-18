from libs.formatter.base_formatter import BaseFormatter
from models.content import ContentItem, ContentType
from models.scrape_task import FormatType
from typing import List
from datetime import datetime



class MarkdownFormatter(BaseFormatter):
    """
    Converts a Content object into well-formatted Markdown.
    """


    def __init__(self, url:str):
        super().__init__(url, FormatType.MARKDOWN)


    def get_doc(self, content: List[ContentItem]) -> str:
        """
        Convert Content object into a well-structured Markdown string.
        """
        lines = []
       
        # Add document header with metadata
        lines.append(self._generate_header(content))
        lines.append("---\n")
       
        # Process content items
        lines.append(self._process_content_items(content))
       
        # Add footer with source
        lines.append(self._generate_footer(content))
       
        return "\n".join(lines)


    def _generate_header(self, content: List[ContentItem]) -> str:
        """Generate a formatted header with metadata."""
        header_lines = []
       
        # Extract title from URL or use a default
        title = self._extract_title_from_url(self._url)
        header_lines.append(f"# {title}\n")
       
        # Add metadata
        
        formatted_date = datetime.now().strftime("%B %d, %Y at %I:%M %p")
        header_lines.append(f"**Date:** {formatted_date}")
       
        header_lines.append(f"**Source:** [{self._url}]({self._url})")
       
        return "\n".join(header_lines)


    def _generate_footer(self, content: List[ContentItem]) -> str:
        """Generate a formatted footer."""
        return f"\n---\n\n*Document generated from: {self._url}*"


    def _extract_title_from_url(self, url: str) -> str:
        """Extract a readable title from the URL."""
        try:
            # Remove protocol and www
            clean_url = url.replace("https://", "").replace("http://", "").replace("www.", "")
            # Get domain name
            domain = clean_url.split("/")[0].split("?")[0]
            # Capitalize and format
            return domain.replace("-", " ").replace("_", " ").title()
        except:
            return "Document"


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


    def _process_content_items(self, items: List[ContentItem]) -> str:
        """Process all content items and format them appropriately."""
        if not items:
            return ""
       
        lines = []
        consecutive_text_count = 0
       
        for i, item in enumerate(items):
            if item.content_type == ContentType.TEXT:
                # Handle text content with proper spacing
                text = self._format_text_content(item.content)
                if text:
                    lines.append(text)
                    consecutive_text_count += 1
           
            elif item.content_type == ContentType.IMG:
                # Validate image URL
                if not self._is_valid_url(item.content):
                    continue
               
                # Add spacing before images if needed
                if consecutive_text_count > 0:
                    lines.append("")
               
                # Format image with descriptive alt text
                alt_text = self._generate_alt_text(item, i)
                lines.append(f"![{alt_text}]({item.content})")
                lines.append("")  # Add spacing after image
                consecutive_text_count = 0
           
            elif item.content_type == ContentType.URL:
                # Skip internal/anchor links
                if not self._is_valid_url(item.content):
                    continue
               
                # Format URL as a proper link
                link_text = self._generate_link_text(item.content)
                lines.append(f"[{link_text}]({item.content})")
                consecutive_text_count = 0
           
            else:
                # Fallback for unknown types
                lines.append(item.content)
                consecutive_text_count = 0
       
        return "\n\n".join(lines)


    def _format_text_content(self, text: str) -> str:
        """Clean and format text content."""
        if not text:
            return ""
       
        # Strip excessive whitespace while preserving intentional breaks
        text = text.strip()
       
        # Remove multiple consecutive spaces
        while "  " in text:
            text = text.replace("  ", " ")
       
        return text


    def _generate_alt_text(self, item: ContentItem, index: int) -> str:
        """Generate descriptive alt text for images."""
        # filename = os.path.basename(item.content)
        # # Remove extension and format
        # name = os.path.splitext(filename)[0].replace("-", " ").replace("_", " ")
        return f"img not present"


    def _generate_link_text(self, url: str) -> str:
        """Generate descriptive text for URLs."""
        try:
            # Try to extract meaningful text from URL
            path = url.split("://")[-1]
            # Remove domain
            parts = path.split("/", 1)
            if len(parts) > 1:
                text = parts[1].split("?")[0].replace("-", " ").replace("_", " ")
                return text.title() if text else url
        except:
            pass
        return url



