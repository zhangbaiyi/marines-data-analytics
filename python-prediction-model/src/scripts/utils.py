import os


def resolve_path_from_project_root(path: str) -> str:
    """
    Args:
        path (str): Relative path to resolve from project root.

    Raises:
        FileNotFoundError: File specified at the argument 'path' does not exist.

    Returns:
        str: Resolved file path from the project root.
    """
    abs_file_path = os.path.join(os.getcwd(), path)
    if not os.path.exists(os.path.join(os.getcwd(), path)):
        raise FileNotFoundError(f"Error: The file '{abs_file_path}' does not exist.")
    return abs_file_path
