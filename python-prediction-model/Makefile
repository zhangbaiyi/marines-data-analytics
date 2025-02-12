PY3_VENV=py3_predict
MAIN_PYTHON_SCRIPT_PATH=src/scripts/main.py

all: run

run:
	python3 ${MAIN_PYTHON_SCRIPT_PATH}

install:
	pip3 install -r requirements.txt
	python3 -m ipykernel install --user --name ${PY3_VENV} --display-name "Python Prediction-Model Project Kernel"

format:
	pip3 install isort black flake8
	isort *.py src/**/*.py
	black *.py src/**/*.py

clean:
	rm -rf ${PY3_VENV}/
	rm -rf *.egg-info/
	find src/ -type d -name "__pycache__" -exec rm -rf {} +
