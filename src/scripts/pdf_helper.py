import os # Import os module
from markdown_pdf import MarkdownPdf, Section

from src.scripts.utils import construct_path_from_project_root
from src.utils.logging import LOGGER

def generate_pdf(_markdown: str = "") -> str | None: # Return type is path (str) or None on error
    """Generates a PDF from markdown and returns the full path to the saved file."""
    try:
        pdf = MarkdownPdf(toc_level=2)

        # Define path (consider making the filename unique if needed, e.g., with a timestamp)
        pdf_name = "generated_report.pdf" # Maybe a slightly more descriptive temporary name
        output_dir = construct_path_from_project_root("output")
        pdf_file_path = os.path.join(output_dir, pdf_name) # Use os.path.join

        # Ensure the output directory exists
        os.makedirs(output_dir, exist_ok=True)

        LOGGER.debug(f"Attempting to generate PDF at: {pdf_file_path}")

        # Add a simple wrapper/title if the markdown doesn't have one
        # Avoid putting the entire markdown inside another header if it already has structure
        # markdown_text = f"# Generated Report\n{_markdown}" # You might adjust this formatting
        markdown_text = _markdown # Use the generated markdown directly

        css_styling = "table, th, td {border: 1px solid black;}"
        pdf.add_section(Section(text=markdown_text), user_css=css_styling)

        pdf.save(pdf_file_path)
        LOGGER.info(f"PDF saved successfully to {pdf_file_path}")
        return pdf_file_path # <-- Return the FULL PATH

    except Exception as e:
        LOGGER.error(f"Error generating or saving PDF: {e}", exc_info=True) # Log traceback
        return None # Indicate failure