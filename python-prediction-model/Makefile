VENV_NM=.venv
MAIN_PYTHON_SCRIPT_PATH=src/scripts/main.py

all: run

run:
	streamlit run ${MAIN_PYTHON_SCRIPT_PATH}

install:
	pip3 install -r requirements.txt
	python3 -m ipykernel install --user --name ${VENV_NM} --display-name "Marines Kernel"

clean:
	rm -rf ${VENV_NM}/
	rm -rf *.egg-info/
	find src/ -type d -name "__pycache__" -exec rm -rf {} +
