from typing import List

import setuptools

# Constants
HYPHEN_E_DOT = "-e ."  # Needed for local Python package installations to
# reflect directly within your Python environment
# ** PLEASE NOTE: DO NOT REMOVE FROM LAST LINE OF `requirements.txt` **


def get_long_description(file_path: str) -> str:
    """
    Reads the `README.md` file associated with this GitHub Repository and
    returns the content as a string.
    """
    with open(file_path, "r") as file_handle:
        return file_handle.read()


def get_requirements(file_path: str) -> List[str]:
    """
    Reads the `requirements.txt` file and return a list of the external Python
    package requirements to install.
    """
    requirements: List[str] = []
    with open(file_path, "r") as file_handle:
        for line in file_handle.readlines():
            requirements.append(line.replace("\n", ""))
        if HYPHEN_E_DOT in requirements:
            requirements.remove(HYPHEN_E_DOT)

    return requirements


# `setup.py` Reference Documentation: https://youtu.be/Rv6UFGNmNZg?si=9aqqoCsgu2XkW6pY
# Classifiers: https://pypi.org/classifiers/
setuptools.setup(
    name="mccs-market-analytics-report-generation-project",
    version="0.0.1",
    author="Andrew Tran",
    author_email="andrewtranva@gmail.com",
    description="An Automated Marking Analytics Assessment Report Generator for MCCS",
    long_description=get_long_description("README.md"),
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),
    install_requires=get_requirements("requirements.txt"),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
        "License :: OSI Approved :: Apache Software License",
    ],
    python_requires=">=3.10",
)
