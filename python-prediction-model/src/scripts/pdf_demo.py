import os

from markdown_pdf import MarkdownPdf, Section

from src.utils.logging import LOGGER


def generate_pdf(_markdown: str = "") -> str:
    pdf = MarkdownPdf(toc_level=2)
    markdown_text = f"""# {_markdown}
"""
    css_styling = "table, th, td {border: 1px solid black;}"
    pdf.add_section(Section(text=markdown_text), user_css=css_styling)
    LOGGER.debug(os.getcwd())
    # Determine PDF-prefix (e.g. '../../' or something else)
    pdf_prefix = "../final-submission"
    pdf_name = "PDF.pdf"
    file_path = f"{pdf_prefix}/{pdf_name}"
    LOGGER.debug(f"PDF (Relative) FILE PATH: {file_path}")
    pdf.save(file_path)
    return pdf_name
