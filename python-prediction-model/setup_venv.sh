#!/bin/bash
# setup_venv.sh
# This script sets up a Python3 virtual environment on Linux or macOS.
# Usage: ./setup_venv.sh <venv_directory>

# Exit immediately if a command exits with a non-zero status.
set -e

# Check if Python3 is installed
if ! command -v python3 &>/dev/null; then
    echo "Error: Python3 is not installed. Please install Python3 and try again."
    exit 1
fi

VENV_DIR="py3_predict"

# Check if the name starts with "py3". If not, prompt until a valid name is given.
while [[ $VENV_DIR != py3* ]]; do
  echo "Error: The name '$VENV_DIR' does not start with 'py3'."
  VENV_DIR=$(prompt_for_venv_name)
done

echo "Creating Python3 virtual environment in: $VENV_DIR"
python3 -m venv "$VENV_DIR"

echo "Virtual environment created successfully."

# Activate the virtual environment
# Note: Activation only affects the current shell session.
echo "Activating virtual environment..."
# shellcheck source=/dev/null
source ./$VENV_DIR/bin/activate

echo "Upgrading pip, setuptools, and wheel..."
pip3 install --upgrade pip setuptools wheel

# Add CWD to the PYTHONPATH environment variable
export PYTHONPATH="${PYTHONPATH}:${PWD}"

echo "Virtual environment is ready and activated!"
echo "To deactivate the environment later, run: \"deactivate\""

echo "Install all the required Python package depenedencies"
make install
