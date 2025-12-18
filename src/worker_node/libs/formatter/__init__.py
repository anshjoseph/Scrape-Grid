from libs.formatter.base_formatter import BaseFormatter
from libs.formatter.json_formatter import JsonFormatter
from libs.formatter.markdown_formatter import MarkdownFormatter
from models.scrape_task import FormatType
from models.content import ContentItem
from typing import List

def format_html(url:str, content:List[ContentItem], format : FormatType) -> str:
    match (format):
        case FormatType.JSON:
            return JsonFormatter(url).get_doc(content)
        case FormatType.MARKDOWN:
            return MarkdownFormatter(url).get_doc(content)