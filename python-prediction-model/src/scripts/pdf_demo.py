from markdown_pdf import MarkdownPdf, Section

from src.scripts.utils import construct_path_from_project_root
from src.utils.logging import LOGGER


def generate_pdf(_markdown: str = "") -> str:
    pdf = MarkdownPdf(toc_level=2)
    markdown_text = f"""# {_markdown}
"""
    css_styling = "table, th, td {border: 1px solid black;}"
    pdf.add_section(Section(text=markdown_text), user_css=css_styling)

    # Determine PDF-prefix (e.g. '../../' or something else)
    pdf_name = "PDF.pdf"
    pdf_file_path = construct_path_from_project_root(f"../final-submission/{pdf_name}")
    LOGGER.debug(f"PDF FILE PATH: {pdf_file_path}")
    pdf.save(pdf_file_path)
    return pdf_name
