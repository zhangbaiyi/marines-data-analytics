#!/bin/bash
# format.sh
# This script formats and lints all Python files recursively.
# Usage: ./format.sh

pip3 install isort black flake8 autopep8

flake8 --max-line-length 120 *.py src/**/*.py
isort *.py src/**/*.py 
black *.py src/**/*.py --line-length 120
find src/ -name '*.py' -exec autopep8 --in-place '{}' \;
