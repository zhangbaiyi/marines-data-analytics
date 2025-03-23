from typing import List

import setuptools

# Constants
HYPHEN_E_DOT = "-e ."  # Needed for local Python package installations to
# reflect directly within your Python environment
# ** PLEASE NOTE: DO NOT REMOVE FROM LAST LINE OF `requirements.txt` **


def get_long_description(file_path: str) -> str:
    with open(file_path, "r") as file_handle:
        return file_handle.read()


def get_requirements(file_path: str) -> List[str]:
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
    name="streamlit-datawarehouse",
    version="1.0.0",
    author="Baiyi Zhang",
    author_email="baiyizhang23@gmail.com",
    description="A Streamlit application for data warehouse interaction and prediction model.",
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
