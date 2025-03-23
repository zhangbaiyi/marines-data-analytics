#!/bin/bash
# setup_venv.sh
# This script sets up a Python3 virtual environment on Linux or macOS.
# Usage: ./setup_venv.sh

# Exit immediately if a command exits with a non-zero status.
set -e

# Check if Python3 is installed
if ! command -v python3 &>/dev/null; then
  echo "Error: Python3 is not installed. Please install Python3 and try again."
  exit 1
fi

# Check if the number of arguments is greater than 0
if [ "$#" -gt 0 ]; then
  echo "Usage: ./setup_venv.sh"
  echo "Error: Too many arguments. Expected no arguments for this script, but got $#."
  exit 1  # Exit with an error code
fi

VENV_DIR=".venv"

# Check if the name starts with "py3". If not, prompt until a valid name is given.
while [[ $VENV_DIR != .venv* ]]; do
  echo "Error: The name '$VENV_DIR' does not start with '.venv'."
  VENV_DIR=$(prompt_for_venv_name)
done

echo "Creating Python3 virtual environment in: $VENV_DIR"
python3.12 -m venv "$VENV_DIR"

echo "Virtual environment created successfully."

# Activate the virtual environment
# Note: Activation only affects the current shell session.
echo "Activating virtual environment..."
# shellcheck source=/dev/null
source ./$VENV_DIR/bin/activate

echo "Upgrading pip and wheel..."
pip3 install --upgrade pip wheel

# Add CWD to the PYTHONPATH environment variable
export PYTHONPATH="${PYTHONPATH}:${PWD}"

echo "Virtual environment is ready and activated!"
echo "To deactivate the environment later, run: \"deactivate\""

echo "Install all the required Python package depenedencies"
make install
