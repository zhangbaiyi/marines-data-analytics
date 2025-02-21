from markdown_pdf import MarkdownPdf, Section
import os
from src.utils.logging import LOGGER

def generate_pdf(_markdown = ""):
    pdf = MarkdownPdf(toc_level=2)
    text = f"""# {_markdown}
"""
    css = "table, th, td {border: 1px solid black;}"
    pdf.add_section(Section(text), user_css=css)
    LOGGER.debug(os.getcwd())
    LOGGER.debug(os.getcwd())
    # Determine PDF-prefix (e.g. '../../' or something else)
    pdf_prefix = "../final-submission" 
    pdf_name = "PDF.pdf"
    file_path = f"{pdf_prefix}/{pdf_name}"
    pdf.save(file_path)
    return pdf_name

    