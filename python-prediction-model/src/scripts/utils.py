import os
from datetime import datetime


def construct_path_from_project_root(rel_file_path: str) -> str:
    """
    Args:
        rel_file_path (str): Relative path to construct from project root.
    Returns:
        str: Constructed file path from the project root.
    """
    return os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../../", rel_file_path)
    )


def resolve_import_path_from_project_root(rel_file_path: str) -> str:
    """
    Args:
        rel_file_path (str): Relative path to resolve from project root.
    Raises:
        FileNotFoundError: File specified at the argument 'path' does not exist.
    Returns:
        str: Resolved file path from the project root.
    """
    abs_file_path = construct_path_from_project_root(rel_file_path)
    if not os.path.exists(abs_file_path):
        raise FileNotFoundError(
            f"Error: The file '{abs_file_path}' does not exist.")
    return abs_file_path


def generate_curr_date_to_append_to_filename() -> str:
    """
    Returns:
        str: Current date in the format 'YYYY_MM_DD'.
    """
    return datetime.now().strftime("%Y_%m_%d")
